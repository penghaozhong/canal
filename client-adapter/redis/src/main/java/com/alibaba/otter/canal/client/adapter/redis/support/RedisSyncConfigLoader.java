package com.alibaba.otter.canal.client.adapter.redis.support;

import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ES 配置装载器
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class RedisSyncConfigLoader {

    private static Logger logger = LoggerFactory.getLogger(RedisSyncConfigLoader.class);

    public static synchronized Map<String, RedisConfig> load(Properties envProperties) {
        logger.info("## Start loading es mapping config ... ");

        Map<String, RedisConfig> esSyncConfig = new LinkedHashMap<>();

        Map<String, String> configContentMap = MappingConfigsLoader.loadConfigs("es");
        configContentMap.forEach((fileName, content) -> {
            RedisConfig config = YmlConfigBinder.bindYmlToObj(null, content, RedisConfig.class, null, envProperties);
            if (config == null) {
                return;
            }
            try {
                config.validate();
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + fileName + " " + e.getMessage(), e);
            }
            esSyncConfig.put(fileName, config);
        });

        logger.info("## ES mapping config loaded");
        return esSyncConfig;
    }
}
