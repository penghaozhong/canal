package com.alibaba.otter.canal.client.adapter.redis.support;

import org.springframework.data.redis.core.RedisTemplate;

/**
 * LIST 类型
 */
public class RedisListOpsServiceImpl implements RedisOpsSerevice {


    private RedisTemplate redisTemplate;

    public RedisListOpsServiceImpl(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override public void add(Object key, Object value) {
        redisTemplate.boundListOps(key).leftPush(value);
    }

    @Override public void delete(Object key, Object value) {
        redisTemplate.boundListOps(key).remove(1, value);
    }
}
