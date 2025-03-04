/*
 * Copyright 2013-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.cloud.nacos.refresh;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.cloud.nacos.NacosConfigManager;
import com.alibaba.cloud.nacos.NacosConfigProperties;
import com.alibaba.cloud.nacos.NacosPropertySourceRepository;
import com.alibaba.cloud.nacos.client.NacosPropertySource;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.AbstractSharedListener;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.context.refresh.ContextRefresher;
import org.springframework.cloud.endpoint.event.RefreshEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;

/**
 * On application start up, NacosContextRefresher add nacos listeners to all application
 * level dataIds, when there is a change in the data, listeners will refresh
 * configurations.
 *
 * @author juven.xuxb
 * @author pbting
 * @author freeman
 *
 * nacos 配置刷新器
 * 在应用程序启动时，NacosContextRefresher 将 nacos 监听器添加到所有应用程序级别的 dataId 中，
 * 当数据发生变化时，监听器会刷新配置。
 */
public class NacosContextRefresher
		implements ApplicationListener<ApplicationReadyEvent>, ApplicationContextAware {

	private final static Logger log = LoggerFactory
			.getLogger(NacosContextRefresher.class);

	/**
	 * nacos 上下文中配置的动态刷新数量
	 */
	private static final AtomicLong REFRESH_COUNT = new AtomicLong(0);

	/**
	 * NacosConfigProperties nacos 配置属性
	 */
	private NacosConfigProperties nacosConfigProperties;

	/**
	 * 是否开启刷新
	 */
	private final boolean isRefreshEnabled;

	/**
	 * nacos 配置刷新历史记录
	 */
	private final NacosRefreshHistory nacosRefreshHistory;

	/**
	 * NacosConfigService
	 */
	private final ConfigService configService;

	/**
	 * spring 容器
	 */
	private ApplicationContext applicationContext;

	/**
	 * 是否已准备
	 */
	private AtomicBoolean ready = new AtomicBoolean(false);

	/**
	 * 监听的集合
	 */
	private Map<String, Listener> listenerMap = new ConcurrentHashMap<>(16);

	public NacosContextRefresher(NacosConfigManager nacosConfigManager,
			NacosRefreshHistory refreshHistory) {
		this.nacosConfigProperties = nacosConfigManager.getNacosConfigProperties();
		this.nacosRefreshHistory = refreshHistory;
		this.configService = nacosConfigManager.getConfigService();
		this.isRefreshEnabled = this.nacosConfigProperties.isRefreshEnabled();
	}

	/**
	 * ApplicationReadyEvent 回调
	 * @param event
	 */
	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		// many Spring context
		if (this.ready.compareAndSet(false, true)) {
			// 注册 nacos 监听器
			this.registerNacosListenersForApplications();
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	/**
	 * register Nacos Listeners.
	 *
	 * 注册 nacos 监听器
	 */
	private void registerNacosListenersForApplications() {
		// 开启动态刷新
		if (isRefreshEnabled()) {
			// 获取所有 NacosPropertySource
			for (NacosPropertySource propertySource : NacosPropertySourceRepository
					.getAll()) {
				// 如果 NacosPropertySource 不支持动态刷新，跳过
				if (!propertySource.isRefreshable()) {
					continue;
				}
				// 获取 dataId
				String dataId = propertySource.getDataId();
				// 注册监听
				registerNacosListener(propertySource.getGroup(), dataId);
			}
		}
	}

	/**
	 * 注册 nacos 监听
	 * @param groupKey group
	 * @param dataKey dateId
	 *
	 * 最终刷新时发布 {@link RefreshEvent}, 由 {@link org.springframework.cloud.endpoint.event.RefreshEventListener#handle(RefreshEvent)}
	 * 进行处理，委托给 {@link ContextRefresher#refresh()} 进行刷新
	 */
	private void registerNacosListener(final String groupKey, final String dataKey) {
		// 获取 key
		String key = NacosPropertySourceRepository.getMapKey(dataKey, groupKey);
		// 对 key 绑定监听事件
		Listener listener = listenerMap.computeIfAbsent(key,
				lst -> new AbstractSharedListener() {
					@Override
					public void innerReceive(String dataId, String group,
							String configInfo) {
						// 递增刷新数量
						refreshCountIncrement();
						// 添加刷新记录，提供端点访问
						nacosRefreshHistory.addRefreshRecord(dataId, group, configInfo);
						// 发布一个刷新事件，用于同步 @Value 注解配置的属性值
						applicationContext.publishEvent(
								new RefreshEvent(this, null, "Refresh Nacos config"));
						if (log.isDebugEnabled()) {
							log.debug(String.format(
									"Refresh Nacos config group=%s,dataId=%s,configInfo=%s",
									group, dataId, configInfo));
						}
					}
				});
		try {
			// 注册监听器
			configService.addListener(dataKey, groupKey, listener);
			log.info("[Nacos Config] Listening config: dataId={}, group={}", dataKey,
					groupKey);
		}
		catch (NacosException e) {
			log.warn(String.format(
					"register fail for nacos listener ,dataId=[%s],group=[%s]", dataKey,
					groupKey), e);
		}
	}

	public NacosConfigProperties getNacosConfigProperties() {
		return nacosConfigProperties;
	}

	public NacosContextRefresher setNacosConfigProperties(
			NacosConfigProperties nacosConfigProperties) {
		this.nacosConfigProperties = nacosConfigProperties;
		return this;
	}

	public boolean isRefreshEnabled() {
		if (null == nacosConfigProperties) {
			return isRefreshEnabled;
		}
		// Compatible with older configurations
		if (nacosConfigProperties.isRefreshEnabled() && !isRefreshEnabled) {
			return false;
		}
		return isRefreshEnabled;
	}

	public static long getRefreshCount() {
		return REFRESH_COUNT.get();
	}

	public static void refreshCountIncrement() {
		REFRESH_COUNT.incrementAndGet();
	}

}
