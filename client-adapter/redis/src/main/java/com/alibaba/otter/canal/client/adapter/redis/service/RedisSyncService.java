package com.alibaba.otter.canal.client.adapter.redis.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.support.spring.FastJsonRedisSerializer;
import com.alibaba.otter.canal.client.adapter.redis.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.googlecode.aviator.AviatorEvaluator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisConnectionUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPoolConfig;

/**
 * redis 同步操作业务
 */
public class RedisSyncService {
    private static final Logger logger  = LoggerFactory.getLogger(RedisSyncService.class);

    private RedisTemplate redisTemplate;

    public RedisSyncService(RedisStandaloneConfiguration configuration) {

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(200);
        config.setMaxIdle(50);
        config.setMinIdle(8);
        config.setMaxWaitMillis(10000);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        JedisClientConfiguration jedisClientConfiguration = JedisClientConfiguration.builder()
            .usePooling()
            .poolConfig(config)
            .build();

        JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory(configuration, jedisClientConfiguration);
        jedisConnectionFactory.afterPropertiesSet();

        redisTemplate =new RedisTemplate();
        redisTemplate.setConnectionFactory(jedisConnectionFactory);
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        redisTemplate.setKeySerializer(stringRedisSerializer);
        FastJsonRedisSerializer<Object> fastJsonRedisSerializer = new FastJsonRedisSerializer<>(Object.class);
        redisTemplate.setValueSerializer(fastJsonRedisSerializer);
        redisTemplate.afterPropertiesSet();

        RedisConnectionUtils.getConnection(jedisConnectionFactory).close();
    }


    public void sync(MappingConfig config, Dml dml) {
        if (config != null) {
            String type = dml.getType();
            if (type != null && type.equalsIgnoreCase("INSERT")) {
                logger.info("{}",dml);
                insert(config,dml);
            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                logger.info("{}",dml);
                update(config, dml);
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                logger.info("{}",dml);
            }
        }
    }

    /**
     * 插入操作
     *
     * @param config 配置项
     * @param dml DML数据
     */
    private void insert(MappingConfig config, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        MappingConfig.RedisMapping hbaseMapping =config.getRedisMapping();

        for (Map<String, Object> r : data) {
            redisTemplate.opsForValue().set(hbaseMapping.getKey(), JSON.toJSON(r));
        }
    }

    private void update(MappingConfig config, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        MappingConfig.RedisMapping hbaseMapping =config.getRedisMapping();

        for (Map<String, Object> r : data) {
            String key = (String)AviatorEvaluator.execute(hbaseMapping.getKey(), r);
            logger.info("redisTemplate-key={}",key);
            redisTemplate.opsForValue().set(key, r);
        }
    }
}
