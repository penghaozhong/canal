package com.alibaba.otter.canal.client.adapter.redis;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.redis.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.redis.config.MappingConfigLoader;
import com.alibaba.otter.canal.client.adapter.redis.service.RedisSyncService;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;
import java.util.ArrayList;
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
                        + mappingConfig.getRedisMapping().getDatabase() + "-"
                        + mappingConfig.getRedisMapping().getTable();
                }
                else {
                    k = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "_"
                        + mappingConfig.getRedisMapping().getDatabase() + "-"
                        + mappingConfig.getRedisMapping().getTable();
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
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        for (Dml dml : dmls) {
            sync(dml);
        }
    }

    private void sync(Dml dml) {
        if (dml == null) {
            return;
        }
        String destination = StringUtils.trimToEmpty(dml.getDestination());
        String groupId = StringUtils.trimToEmpty(dml.getGroupId());
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, MappingConfig> configMap;
        if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
            configMap = mappingConfigCache.get(destination + "-" + groupId + "_" + database + "-" + table);
        } else {
            configMap = mappingConfigCache.get(destination + "_" + database + "-" + table);
        }
        if (configMap != null) {
            List<MappingConfig> configs = new ArrayList<>();
            configMap.values().forEach(config -> {
                if (StringUtils.isNotEmpty(config.getGroupId())) {
                    if (config.getGroupId().equals(dml.getGroupId())) {
                        configs.add(config);
                    }
                } else {
                    configs.add(config);
                }
            });
            if (!configs.isEmpty()) {
                configs.forEach(config -> redisSyncService.sync(config, dml));
            }
        }
    }

    @Override public void destroy() {

    }
}
