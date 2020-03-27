package com.alibaba.otter.canal.client.adapter.redis.service;

import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.support.collections.RedisProperties;
import redis.clients.jedis.JedisPoolConfig;

public class RedisSyncService {

    private JedisPoolConfig jedisPoolConfig;
    private JedisConnectionFactory jedisConnectionFactory;
    private RedisTemplate redisTemplate;

    public RedisSyncService(RedisStandaloneConfiguration configuration) {
        jedisConnectionFactory = new JedisConnectionFactory(configuration);
        redisTemplate =new RedisTemplate();
        redisTemplate.setConnectionFactory(jedisConnectionFactory);
    }
}
