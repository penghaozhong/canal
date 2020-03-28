package com.alibaba.otter.canal.client.adapter.redis;

import com.alibaba.fastjson.support.spring.FastJsonRedisSerializer;
import com.alibaba.otter.canal.client.adapter.redis.config.MappingConfig;
import java.sql.SQLException;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Ignore
public class DBTest {

    @Test
    public void test01() throws SQLException {

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(200);
        config.setMaxIdle(50);
        config.setMinIdle(8);//设置最小空闲数
        config.setMaxWaitMillis(10000);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration();
//        standaloneConfiguration.setHostName("10.12.0.40");
        standaloneConfiguration.setHostName("localhost");
        standaloneConfiguration.setPort(6379);
        standaloneConfiguration.setDatabase(0);
        standaloneConfiguration.setPassword(RedisPassword.of("MBkMl4cssBcbet1W"));

        JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory(standaloneConfiguration);
        jedisConnectionFactory.setPoolConfig(config);

        System.out.println("jedisConnectionFactory.getDatabase() = " + jedisConnectionFactory.getDatabase());

        jedisConnectionFactory.afterPropertiesSet();

        RedisTemplate redisTemplate = new RedisTemplate();
        redisTemplate.setConnectionFactory(jedisConnectionFactory);
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        redisTemplate.setKeySerializer(stringRedisSerializer);
        redisTemplate.setValueSerializer(new FastJsonRedisSerializer<>(Object.class));
        redisTemplate.afterPropertiesSet();


        redisTemplate.opsForValue().set("test11111","787878");

        Object test11111 = redisTemplate.opsForValue().get("test11111");
        System.out.println("test11111 = " + test11111);

        Object teee = redisTemplate.opsForValue().get("teee");
        System.out.println("teee = " + teee);

//        redisTemplate.boundListOps("test45").leftPush("999900");
        System.out.println("redisTemplate-range= " + redisTemplate.boundListOps("test45").range(0, 100));

        MappingConfig mappingConfig = new MappingConfig();
        mappingConfig.setDestination("hah");
        mappingConfig.setGroupId("g1");
        redisTemplate.opsForValue().set("testobject",mappingConfig);

        System.out.println("testobject=" + redisTemplate.opsForValue().get("testobject"));

    }
}
