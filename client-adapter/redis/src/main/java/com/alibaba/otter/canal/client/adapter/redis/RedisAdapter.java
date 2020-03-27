package com.alibaba.otter.canal.client.adapter.redis;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.redis.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.redis.config.MappingConfigLoader;
import com.alibaba.otter.canal.client.adapter.redis.service.RedisSyncService;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.StringUtils;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;

@SPI("redis")
public class RedisAdapter implements OuterAdapter {

    private Properties  envProperties;
    private Map<String, MappingConfig> redisMapping = new ConcurrentHashMap<>();                  // 文件名对应配置
    private Map<String, Map<String, MappingConfig>> mappingConfigCache = new ConcurrentHashMap<>();


    private RedisSyncService redisSyncService;

    @Override public void init(OuterAdapterConfig configuration, Properties envProperties) {
        try {
            this.envProperties = envProperties;
            Map<String, MappingConfig> hbaseMappingTmp = MappingConfigLoader.load(envProperties);
            // 过滤不匹配的key的配置
            hbaseMappingTmp.forEach((key, mappingConfig) -> {
                if ((mappingConfig.getOuterAdapterKey() == null && configuration.getKey() == null)
                    || (mappingConfig.getOuterAdapterKey() != null
                    && mappingConfig.getOuterAdapterKey().equalsIgnoreCase(configuration.getKey()))) {
                    redisMapping.put(key, mappingConfig);
                }
            });
            for (Map.Entry<String, MappingConfig> entry : redisMapping.entrySet()) {
                String configName = entry.getKey();
                MappingConfig mappingConfig = entry.getValue();
                String k;
                if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                    k = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "-"
                        + StringUtils.trimToEmpty(mappingConfig.getGroupId()) + "_"
                        + mappingConfig.getRedisMapping().getKey();
                } else {
                    k = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "_"
                        + mappingConfig.getRedisMapping().getKey();
                }
                Map<String, MappingConfig> configMap = mappingConfigCache.computeIfAbsent(k,
                    k1 -> new ConcurrentHashMap<>());
                configMap.put(configName, mappingConfig);
            }

            Map<String, String> properties = configuration.getProperties();

            //初始化redisTemplate
            RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration();
            standaloneConfiguration.setHostName(properties.get("hostName"));
            standaloneConfiguration.setPort(Integer.parseInt(properties.get("port")));
            standaloneConfiguration.setPassword(RedisPassword.of(properties.get("password")));

            redisSyncService= new RedisSyncService(standaloneConfiguration);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override public void sync(List<Dml> dmls) {

    }

    @Override public void destroy() {

    }
}
