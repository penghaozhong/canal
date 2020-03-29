package com.alibaba.otter.canal.client.adapter.redis.support;

import java.util.HashMap;
import java.util.Map;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * redis数据类型策略类
 */
public class RedisOpsStrategy {

    private Map<String,RedisOpsSerevice> sereviceMap;

    public RedisOpsStrategy(RedisTemplate redisTemplate) {
        sereviceMap = new HashMap<>();
        sereviceMap.put(RedisType.GEO.name(), new RedisGeoOpsServiceImpl(redisTemplate));
        sereviceMap.put(RedisType.HASH.name(), new RedisHashOpsServiceImpl(redisTemplate));
        sereviceMap.put(RedisType.LIST.name(), new RedisListOpsServiceImpl(redisTemplate));
        sereviceMap.put(RedisType.SET.name(), new RedisSetOpsServiceImpl(redisTemplate));
        sereviceMap.put(RedisType.STRING.name(), new RedisStringOpsServiceImpl(redisTemplate));
        sereviceMap.put(RedisType.ZSET.name(), new RedisZSetOpsServiceImpl(redisTemplate));
    }

    public RedisOpsSerevice getService(String keyType) {
        RedisOpsSerevice serevice = sereviceMap.get(keyType);
        if (serevice == null) {
            serevice = sereviceMap.get(RedisType.STRING.name());
        }
        return serevice;
    }

}
