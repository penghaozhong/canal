package com.alibaba.otter.canal.client.adapter.redis.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.adapter.redis.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * redis 同步操作业务
 */
public class RedisSyncService {
    private static final Logger logger  = LoggerFactory.getLogger(RedisSyncService.class);

    private JedisConnectionFactory jedisConnectionFactory;
    private RedisTemplate redisTemplate;

    public RedisSyncService(RedisStandaloneConfiguration configuration) {
        jedisConnectionFactory = new JedisConnectionFactory(configuration);
        redisTemplate =new RedisTemplate();
        redisTemplate.setConnectionFactory(jedisConnectionFactory);
    }


    public void sync(MappingConfig config, Dml dml) {
        if (config != null) {
            String type = dml.getType();
            if (type != null && type.equalsIgnoreCase("INSERT")) {
                logger.info("{}",dml);
                insert(config,dml);
            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                logger.info("{}",dml);
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
}
