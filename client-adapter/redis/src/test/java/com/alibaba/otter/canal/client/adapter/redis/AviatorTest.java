package com.alibaba.otter.canal.client.adapter.redis;

import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import java.util.Map;

public class AviatorTest {
    public static void main(String[] args) {
        Map<String, Object> map = Maps.newConcurrentMap();
        map.put("level", 2);
        boolean result = (boolean)AviatorEvaluator.execute("level==2", map);
        System.out.println("result = " + result);
    }
}
