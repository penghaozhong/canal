package com.alibaba.otter.canal.client.adapter.redis.support;

import org.springframework.data.redis.core.RedisTemplate;

/**
 * string 类型
 */
public class RedisStringOpsServiceImpl implements RedisOpsSerevice {


    private RedisTemplate redisTemplate;

    public RedisStringOpsServiceImpl(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override public void add(Object key, Object value) {
        redisTemplate.boundValueOps(key).set(value);
    }

    @Override public void delete(Object key, Object value) {
        redisTemplate.delete(key);
    }
}
