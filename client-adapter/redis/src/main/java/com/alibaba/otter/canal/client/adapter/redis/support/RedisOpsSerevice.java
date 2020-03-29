package com.alibaba.otter.canal.client.adapter.redis.support;

/**
 * redis操作接口
 */
public interface RedisOpsSerevice {

    void add(Object key, Object value);
    void delete(Object key, Object value);


}
