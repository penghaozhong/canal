package com.alibaba.otter.canal.client.adapter.redis;

import java.sql.SQLException;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
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
        config.setTestWhileIdle(true);
        config.setTimeBetweenEvictionRunsMillis(30000);
        config.setNumTestsPerEvictionRun(10);
        config.setMinEvictableIdleTimeMillis(60000);
        JedisPool pool = new JedisPool(config, "10.12.0.40", 6379, 10000, "MBkMl4cssBcbet1W", 0);

        JedisPoolConfig configuration = new JedisPoolConfig();
        RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration();
        standaloneConfiguration.setHostName("10.12.0.40");
        standaloneConfiguration.setPort(6379);

        standaloneConfiguration.setPassword(RedisPassword.of("MBkMl4cssBcbet1W"));

        JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory(config);

        jedisConnectionFactory.afterPropertiesSet();

        RedisTemplate redisTemplate = new RedisTemplate();
        redisTemplate.setConnectionFactory(jedisConnectionFactory);
        redisTemplate.afterPropertiesSet();


        redisTemplate.opsForValue().set("test11111","33333");
    }
}
