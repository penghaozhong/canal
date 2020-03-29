package com.alibaba.otter.canal.client.adapter.redis.support;

import java.util.Map;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * GEO 类型
 */
public class RedisGeoOpsServiceImpl implements RedisOpsSerevice {


    private RedisTemplate redisTemplate;

    public RedisGeoOpsServiceImpl(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override public void add(Object key, Object value) {
        redisTemplate.boundGeoOps(key).add((Map)value);
    }

    @Override public void delete(Object key, Object value) {
        redisTemplate.delete(key);
    }
}
