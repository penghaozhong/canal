package com.alibaba.otter.canal.client.adapter.redis;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main(String[] args) {
        String expression = "level==2";
        // 编译表达式
        Expression compiledExp = AviatorEvaluator.compile(expression);
        Map<String, Object> env = new HashMap<String, Object>();
        env.put("level", 2);
        // 执行表达式
        Boolean result = (Boolean) compiledExp.execute(env);
        System.out.println(result);  // false
    }
}
