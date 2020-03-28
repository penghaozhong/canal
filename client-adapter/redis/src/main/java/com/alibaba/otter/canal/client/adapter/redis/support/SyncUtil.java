package com.alibaba.otter.canal.client.adapter.redis.support;

import com.alibaba.otter.canal.client.adapter.redis.config.MappingConfig;
import java.util.HashMap;
import java.util.Map;
import org.springframework.util.CollectionUtils;

public class SyncUtil {

    /**
     * 对象映射方法
     * @param redisMapping
     * @param data
     * @return
     */
    public static Map<String, Object> getTargetMap(MappingConfig.RedisMapping redisMapping, Map<String, Object> data) {
        Map<String, Object> newMap = new HashMap<>();
        if (redisMapping.isMapAll()) {
            return data;
        }

        if (CollectionUtils.isEmpty(redisMapping.getTargetPropertys())) {
            return data;
        }

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            for (Map.Entry<String, String> tentry : redisMapping.getTargetPropertys().entrySet()) {
                if (entry.getKey().equals(tentry.getKey())) {
                    newMap.put(tentry.getValue(), entry.getValue());
                    break;
                }
            }
        }
        return newMap;
    }

}
