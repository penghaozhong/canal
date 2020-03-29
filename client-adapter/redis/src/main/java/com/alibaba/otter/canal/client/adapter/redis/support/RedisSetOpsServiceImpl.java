package com.alibaba.otter.canal.client.adapter.redis.support;

import org.springframework.data.redis.core.RedisTemplate;

/**
 * SET 类型
 */
public class RedisSetOpsServiceImpl implements RedisOpsSerevice {


    private RedisTemplate redisTemplate;

    public RedisSetOpsServiceImpl(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override public void add(Object key, Object value) {
        redisTemplate.boundSetOps(key).add(value);
    }

    @Override public void delete(Object key, Object value) {
        redisTemplate.boundSetOps(key).remove(value);
    }
}
