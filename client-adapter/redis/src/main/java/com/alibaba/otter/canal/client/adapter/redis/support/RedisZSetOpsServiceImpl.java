package com.alibaba.otter.canal.client.adapter.redis.support;

import org.springframework.data.redis.core.RedisTemplate;

/**
 * ZSET 类型
 */
public class RedisZSetOpsServiceImpl implements RedisOpsSerevice {


    private RedisTemplate redisTemplate;

    public RedisZSetOpsServiceImpl(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override public void add(Object key, Object value) {
        redisTemplate.boundZSetOps(key).add(value,System.currentTimeMillis());
    }

    @Override public void delete(Object key, Object value) {
        redisTemplate.boundZSetOps(key).remove(value);
    }
}
