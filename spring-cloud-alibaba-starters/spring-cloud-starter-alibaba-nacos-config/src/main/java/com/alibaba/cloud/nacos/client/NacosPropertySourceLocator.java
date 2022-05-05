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

package com.alibaba.cloud.nacos.client;

import java.util.List;

import com.alibaba.cloud.commons.lang.StringUtils;
import com.alibaba.cloud.nacos.NacosConfigManager;
import com.alibaba.cloud.nacos.NacosConfigProperties;
import com.alibaba.cloud.nacos.NacosPropertySourceRepository;
import com.alibaba.cloud.nacos.parser.NacosDataParserHandler;
import com.alibaba.cloud.nacos.refresh.NacosContextRefresher;
import com.alibaba.nacos.api.config.ConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.bootstrap.config.PropertySourceLocator;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.CompositePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.util.CollectionUtils;

/**
 * @author xiaojing
 * @author pbting
 *
 * nacos 属性加载器，加载远程配置文件
 */
@Order(0)
public class NacosPropertySourceLocator implements PropertySourceLocator {

	private static final Logger log = LoggerFactory
			.getLogger(NacosPropertySourceLocator.class);

	/**
	 * nacos config name, 默认 NACOS
	 */
	private static final String NACOS_PROPERTY_SOURCE_NAME = "NACOS";

	private static final String SEP1 = "-";

	private static final String DOT = ".";

	/**
	 * Nacos PropertySource 构建器
	 */
	private NacosPropertySourceBuilder nacosPropertySourceBuilder;

	/**
	 * Nacos config 配置信息
	 */
	private NacosConfigProperties nacosConfigProperties;

	/**
	 * nacos 配置管理器
	 */
	private NacosConfigManager nacosConfigManager;

	/**
	 * recommend to use
	 * {@link NacosPropertySourceLocator#NacosPropertySourceLocator(com.alibaba.cloud.nacos.NacosConfigManager)}.
	 * @param nacosConfigProperties nacosConfigProperties
	 */
	@Deprecated
	public NacosPropertySourceLocator(NacosConfigProperties nacosConfigProperties) {
		this.nacosConfigProperties = nacosConfigProperties;
	}

	public NacosPropertySourceLocator(NacosConfigManager nacosConfigManager) {
		this.nacosConfigManager = nacosConfigManager;
		this.nacosConfigProperties = nacosConfigManager.getNacosConfigProperties();
	}

	/**
	 * 加载配置信息
	 * @param env The current Environment.
	 * @return PropertySource
	 */
	@Override
	public PropertySource<?> locate(Environment env) {
		nacosConfigProperties.setEnvironment(env);
		// 获取 NacosConfigService
		ConfigService configService = nacosConfigManager.getConfigService();

		if (null == configService) {
			log.warn("no instance of config service found, can't load config from nacos");
			return null;
		}
		// 获取超时时间
		long timeout = nacosConfigProperties.getTimeout();
		// 创建 NacosPropertySourceBuilder
		nacosPropertySourceBuilder = new NacosPropertySourceBuilder(configService,
				timeout);
		// 获取名称
		String name = nacosConfigProperties.getName();
        // 获取 dataId 前缀
		String dataIdPrefix = nacosConfigProperties.getPrefix();
		if (StringUtils.isEmpty(dataIdPrefix)) {
		    // 没有配置 prefix, 将 name 赋值给 dataIdPrefix
			dataIdPrefix = name;
		}
        // 如果没有配置 name 和 prefix
		if (StringUtils.isEmpty(dataIdPrefix)) {
		    // 设置 spring.application.name
			dataIdPrefix = env.getProperty("spring.application.name");
		}
        // 创建复合的 PropertySource，指定名称为 NACOS
		CompositePropertySource composite = new CompositePropertySource(
				NACOS_PROPERTY_SOURCE_NAME);
        // 加载共享配置
		loadSharedConfiguration(composite);
		// 加载扩展配置
		loadExtConfiguration(composite);
		// 加载应用配置
		loadApplicationConfiguration(composite, dataIdPrefix, nacosConfigProperties, env);
		return composite;
	}

	/**
	 * load shared configuration.
     *
     * 加载共享配置
	 */
	private void loadSharedConfiguration(
			CompositePropertySource compositePropertySource) {
	    // 获取所有配置的共享配置
		List<NacosConfigProperties.Config> sharedConfigs = nacosConfigProperties
				.getSharedConfigs();
		// 有共享配置
		if (!CollectionUtils.isEmpty(sharedConfigs)) {
		    // 校验配置
			checkConfiguration(sharedConfigs, "shared-configs");
			// 加载共享配置
			loadNacosConfiguration(compositePropertySource, sharedConfigs);
		}
	}

	/**
	 * load extensional configuration.
     *
     * 加载共享配置
	 */
	private void loadExtConfiguration(CompositePropertySource compositePropertySource) {
	    // 获取配置的扩展配置列表
		List<NacosConfigProperties.Config> extConfigs = nacosConfigProperties
				.getExtensionConfigs();
		// 不为空，则加载
		if (!CollectionUtils.isEmpty(extConfigs)) {
		    // 校验配置
			checkConfiguration(extConfigs, "extension-configs");
			// 加载扩展配置
			loadNacosConfiguration(compositePropertySource, extConfigs);
		}
	}

	/**
	 * load configuration of application.
     *
     * 加载应用配置
	 */
	private void loadApplicationConfiguration(
			CompositePropertySource compositePropertySource, String dataIdPrefix,
			NacosConfigProperties properties, Environment environment) {
	    // 获取配置的扩展名，默认 properties
		String fileExtension = properties.getFileExtension();
		// 获取分组，默认 DEFAULT_GROUP
		String nacosGroup = properties.getGroup();
		// load directly once by default
        // 加载 dataId = spring.application.name 的配置
		loadNacosDataIfPresent(compositePropertySource, dataIdPrefix, nacosGroup,
				fileExtension, true);
		// load with suffix, which have a higher priority than the default
        // 加载 dataId = spring.application.name.fileExtension 的配置
		loadNacosDataIfPresent(compositePropertySource,
				dataIdPrefix + DOT + fileExtension, nacosGroup, fileExtension, true);
		// Loaded with profile, which have a higher priority than the suffix
        // 获取所有环境，循环加载激活的环境配置
		for (String profile : environment.getActiveProfiles()) {
		    // 加载 dataId = spring.application.name_profile.fileExtension 的配置
			String dataId = dataIdPrefix + SEP1 + profile + DOT + fileExtension;
			loadNacosDataIfPresent(compositePropertySource, dataId, nacosGroup,
					fileExtension, true);
		}

	}

    /**
     * 加载共享/扩展配置
     * @param composite CompositePropertySource
     * @param configs 配置列表
     */
	private void loadNacosConfiguration(final CompositePropertySource composite,
			List<NacosConfigProperties.Config> configs) {
	    // 遍历配置
		for (NacosConfigProperties.Config config : configs) {
		    // 如果数据存在，则加载配置
			loadNacosDataIfPresent(composite, config.getDataId(), config.getGroup(),
					NacosDataParserHandler.getInstance()
							.getFileExtension(config.getDataId()),
					config.isRefresh());
		}
	}

    /**
     * 校验配置信息
     * @param configs 共享/扩展配置列表
     * @param tips 类型 shared-configs/extension-configs
     */
	private void checkConfiguration(List<NacosConfigProperties.Config> configs,
			String tips) {
	    // 遍历
		for (int i = 0; i < configs.size(); i++) {
		    // 获取 dataId
			String dataId = configs.get(i).getDataId();
			// 如果 dataId 为空，抛异常
			if (dataId == null || dataId.trim().length() == 0) {
				throw new IllegalStateException(String.format(
						"the [ spring.cloud.nacos.config.%s[%s] ] must give a dataId",
						tips, i));
			}
		}
	}

    /**
     * 加载指定 dataId 的配置
     * @param composite CompositePropertySource
     * @param dataId 需要加载的配置
     * @param group 分组
     * @param fileExtension 文件扩展名
     * @param isRefreshable 是否刷新，默认传的都是 true
     */
	private void loadNacosDataIfPresent(final CompositePropertySource composite,
			final String dataId, final String group, String fileExtension,
			boolean isRefreshable) {
	    // 校验 dataId
		if (null == dataId || dataId.trim().length() < 1) {
			return;
		}
		// 校验 group
		if (null == group || group.trim().length() < 1) {
			return;
		}
		// 加载配置到 NacosPropertySource 中
		NacosPropertySource propertySource = this.loadNacosPropertySource(dataId, group,
				fileExtension, isRefreshable);
		// 添加 PropertySource
		this.addFirstPropertySource(composite, propertySource, false);
	}

    /**
     * 加载配置到 NacosPropertySource 中
     * @param dataId 配置名
     * @param group 分组
     * @param fileExtension 配置扩展名
     * @param isRefreshable 是否刷新
     * @return NacosPropertySource
     */
	private NacosPropertySource loadNacosPropertySource(final String dataId,
			final String group, String fileExtension, boolean isRefreshable) {
		// 如果 nacos 上下文中配置的动态刷新监听数量
		if (NacosContextRefresher.getRefreshCount() != 0) {
		    // 是否支持刷新
			if (!isRefreshable) {
			    // 不支持，从缓存中获取配置
				return NacosPropertySourceRepository.getNacosPropertySource(dataId,
						group);
			}
		}
        // 支持，从 nacos 远程获取配置
		return nacosPropertySourceBuilder.build(dataId, group, fileExtension,
				isRefreshable);
	}

	/**
	 * Add the nacos configuration to the first place and maybe ignore the empty
	 * configuration.
	 */
	private void addFirstPropertySource(final CompositePropertySource composite,
			NacosPropertySource nacosPropertySource, boolean ignoreEmpty) {
		if (null == nacosPropertySource || null == composite) {
			return;
		}
		if (ignoreEmpty && nacosPropertySource.getSource().isEmpty()) {
			return;
		}
		composite.addFirstPropertySource(nacosPropertySource);
	}

	public void setNacosConfigManager(NacosConfigManager nacosConfigManager) {
		this.nacosConfigManager = nacosConfigManager;
	}

}
