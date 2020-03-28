package com.alibaba.otter.canal.client.adapter.redis.support;

import com.alibaba.otter.canal.client.adapter.redis.config.MappingConfig;
import java.util.HashMap;
import java.util.Map;
import org.springframework.util.CollectionUtils;

public class SyncUtil {

    public static Map<String, Object> getTargetMap(MappingConfig.RedisMapping dbMapping, Map<String, Object> data) {
        Map<String, Object> newMap = new HashMap<>();
        if (dbMapping.isMapAll()) {
            return data;
        }

        if (CollectionUtils.isEmpty(dbMapping.getTargetPropertys())) {
            return data;
        }

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            for (Map.Entry<String, String> tentry : dbMapping.getTargetPropertys().entrySet()) {
                if (entry.getKey().equals(tentry.getValue())) {
                    newMap.put(tentry.getKey(), entry.getValue());
                    break;
                }
            }
        }
        return newMap;
    }

}
