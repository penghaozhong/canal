package com.alibaba.otter.canal.client.adapter.redis;

import com.alibaba.fastjson.support.spring.FastJsonRedisSerializer;
import com.alibaba.otter.canal.client.adapter.redis.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.redis.config.MappingConfigLoader;
import java.sql.SQLException;
import java.util.Map;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPoolConfig;

@Ignore
public class MappingConfigLoaderTest {

    @Test
    public void test01() throws SQLException {

        Map<String, MappingConfig> configMap =  MappingConfigLoader.load(null);

        Assert.assertFalse(configMap.isEmpty());
    }
}
