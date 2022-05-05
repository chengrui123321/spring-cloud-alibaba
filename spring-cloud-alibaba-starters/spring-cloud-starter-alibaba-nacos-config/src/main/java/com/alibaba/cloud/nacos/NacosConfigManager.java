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

package com.alibaba.cloud.nacos;

import java.util.Objects;

import com.alibaba.cloud.nacos.diagnostics.analyzer.NacosConnectionFailureException;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zkzlx
 *
 * nacos 配置属性管理器
 */
public class NacosConfigManager {

	private static final Logger log = LoggerFactory.getLogger(NacosConfigManager.class);

	/**
	 * ConfigService 配置服务， nacos 配置核心类
	 * 此处为 NacosConfigService
	 */
	private static ConfigService service = null;

	/**
	 * nacos 配置属性
	 */
	private NacosConfigProperties nacosConfigProperties;

	public NacosConfigManager(NacosConfigProperties nacosConfigProperties) {
		// 设置 NacosConfigProperties
		this.nacosConfigProperties = nacosConfigProperties;
		// Compatible with older code in NacosConfigProperties,It will be deleted in the
		// future.
		// 创建 ConfigService
		createConfigService(nacosConfigProperties);
	}

	/**
	 * Compatible with old design,It will be perfected in the future.
	 *
	 * 创建 ConfigService
	 */
	static ConfigService createConfigService(
			NacosConfigProperties nacosConfigProperties) {
		// ConfigService 为空，创建
		if (Objects.isNull(service)) {
			// 加锁
			synchronized (NacosConfigManager.class) {
				try {
					if (Objects.isNull(service)) {
						// 通过 NacosFactory 创建 NacosConfigService
						service = NacosFactory.createConfigService(
								nacosConfigProperties.assembleConfigServiceProperties());
					}
				}
				catch (NacosException e) {
					log.error(e.getMessage());
					throw new NacosConnectionFailureException(
							nacosConfigProperties.getServerAddr(), e.getMessage(), e);
				}
			}
		}
		return service;
	}

	public ConfigService getConfigService() {
		if (Objects.isNull(service)) {
			createConfigService(this.nacosConfigProperties);
		}
		return service;
	}

	public NacosConfigProperties getNacosConfigProperties() {
		return nacosConfigProperties;
	}

}
