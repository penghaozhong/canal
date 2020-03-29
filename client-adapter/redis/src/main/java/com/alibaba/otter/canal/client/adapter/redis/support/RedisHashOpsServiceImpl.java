package com.alibaba.otter.canal.client.adapter.redis.support;

import java.util.Map;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * HASH 类型
 */
public class RedisHashOpsServiceImpl implements RedisOpsSerevice {


    private RedisTemplate redisTemplate;

    public RedisHashOpsServiceImpl(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override public void add(Object key, Object value) {
        redisTemplate.boundHashOps(key).putAll((Map)value);
    }

    @Override public void delete(Object key, Object value) {
        redisTemplate.delete(key);
    }
}
