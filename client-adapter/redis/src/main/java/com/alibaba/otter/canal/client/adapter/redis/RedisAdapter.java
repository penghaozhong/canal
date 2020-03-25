package com.alibaba.otter.canal.client.adapter.redis;

import com.alibaba.otter.canal.client.adapter.redis.support.RedisConfig;
import com.alibaba.otter.canal.client.adapter.redis.support.RedisSyncConfigLoader;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * penghaozhong
 */
@SPI("redis")
// logger参数对应CanalOuterAdapterConfiguration配置中的name
public class RedisAdapter implements OuterAdapter {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Properties envProperties;
    private RedisClusterConfiguration redisClusterConfiguration;

    private RedisTemplate redisTemplate;

    private JedisConnectionFactory jedisConnectionFactory;
    private Map<String, RedisConfig>              redisMapping       = new ConcurrentHashMap<>();
    private Map<String, Map<String, RedisConfig>> mappingConfigCache  = new ConcurrentHashMap<>();

    public Map<String, RedisConfig> getHbaseMapping() {
        return redisMapping;
    }

    public Map<String, Map<String, RedisConfig>> getMappingConfigCache() {
        return mappingConfigCache;
    }


    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        try {
            this.envProperties = envProperties;
            Map<String, RedisConfig> hbaseMappingTmp = RedisSyncConfigLoader.load(envProperties);
            // 过滤不匹配的key的配置
            hbaseMappingTmp.forEach((key, RedisConfig) -> {
                if ((RedisConfig.getOuterAdapterKey() == null && configuration.getKey() == null)
                    || (RedisConfig.getOuterAdapterKey() != null
                    && RedisConfig.getOuterAdapterKey().equalsIgnoreCase(configuration.getKey()))) {
                    redisMapping.put(key, RedisConfig);
                }
            });
            for (Map.Entry<String, RedisConfig> entry : redisMapping.entrySet()) {
                String configName = entry.getKey();
                RedisConfig RedisConfig = entry.getValue();
                String k;
                if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                    k = StringUtils.trimToEmpty(RedisConfig.getDestination()) + "-"
                        + StringUtils.trimToEmpty(RedisConfig.getGroupId()) + "_"
                        + RedisConfig.getRedisMapping().getTargetRedis() + "-"
                        + RedisConfig.getRedisMapping().getTargetKey();
                } else {
                    k = StringUtils.trimToEmpty(RedisConfig.getDestination()) + "_"
                        + RedisConfig.getRedisMapping().getTargetRedis() + "-"
                        + RedisConfig.getRedisMapping().getTargetKey();
                }
                Map<String, RedisConfig> configMap = mappingConfigCache.computeIfAbsent(k,
                    k1 -> new ConcurrentHashMap<>());
                configMap.put(configName, RedisConfig);
            }

            Map<String, String> properties = configuration.getProperties();

            // 初始化redis

            redisClusterConfiguration = new RedisClusterConfiguration();
            Iterable<RedisNode> nodes = Sets.newHashSet();
            redisClusterConfiguration.setClusterNodes(nodes);
            jedisConnectionFactory = new JedisConnectionFactory(redisClusterConfiguration);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void sync(List<Dml> dmls) {
        for (Dml dml : dmls) {
            sync(dml);
        }
    }

    public void sync(Dml dml) {
        logger.info("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
        redisTemplate.opsForValue().set("",dml);
    }

    @Override
    public void destroy() {
    }
}
