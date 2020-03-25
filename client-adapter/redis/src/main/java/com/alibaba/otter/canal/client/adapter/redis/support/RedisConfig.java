package com.alibaba.otter.canal.client.adapter.redis.support;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;

/**
 * RDB表映射配置
 *
 * @author rewerma 2018-11-07 下午02:41:34
 * @version 1.0.0
 */
public class RedisConfig implements AdapterConfig {

    private String    dataSourceKey;      // 数据源key

    private String    destination;        // canal实例或MQ的topic

    private String    groupId;            // groupId

    private String    outerAdapterKey;    // 对应适配器的key

    private boolean   concurrent = false; // 是否并行同步

    private RedisMapping redisMapping;          // db映射配置

    public String getDataSourceKey() {
        return dataSourceKey;
    }

    public void setDataSourceKey(String dataSourceKey) {
        this.dataSourceKey = dataSourceKey;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getOuterAdapterKey() {
        return outerAdapterKey;
    }

    public void setOuterAdapterKey(String outerAdapterKey) {
        this.outerAdapterKey = outerAdapterKey;
    }

    public boolean getConcurrent() {
        return concurrent;
    }

    public void setConcurrent(boolean concurrent) {
        this.concurrent = concurrent;
    }

    public RedisMapping getRedisMapping() {
        return redisMapping;
    }

    public void setRedisMapping(RedisMapping redisMapping) {
        this.redisMapping = redisMapping;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public AdapterMapping getMapping() {
        return redisMapping;
    }

    public void validate() {
        if (redisMapping.targetRedis == null || redisMapping.targetRedis.isEmpty()) {
            throw new NullPointerException("dbMapping.database");
        }
        if (redisMapping.targetKey == null || redisMapping.targetKey.isEmpty()) {
            throw new NullPointerException("dbMapping.table");
        }
    }

    public static class RedisMapping implements AdapterMapping {

        private String              targetRedis;                         // 目标库名
        private String              targetKey;                            // 目标表字段映射

        private String              etlCondition;                        // etl条件sql

        @Override public String getEtlCondition() {
            return etlCondition;
        }

        public String getTargetRedis() {
            return targetRedis;
        }

        public void setTargetRedis(String targetRedis) {
            this.targetRedis = targetRedis;
        }

        public String getTargetKey() {
            return targetKey;
        }

        public void setTargetKey(String targetKey) {
            this.targetKey = targetKey;
        }
    }
}
