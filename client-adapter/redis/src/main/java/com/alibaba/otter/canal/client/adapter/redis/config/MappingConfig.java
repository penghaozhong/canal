package com.alibaba.otter.canal.client.adapter.redis.config;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

/**
 * RDB表映射配置
 *
 * @author rewerma 2018-11-07 下午02:41:34
 * @version 1.0.0
 */
public class MappingConfig implements AdapterConfig {

    private String    dataSourceKey;      // 数据源key

    private String    destination;        // canal实例或MQ的topic

    private String    groupId;            // groupId

    private String    outerAdapterKey;    // 对应适配器的key

    private RedisMapping redisMapping;          // redis映射配置

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


    public RedisMapping getRedisMapping() {
        return redisMapping;
    }

    public void setRedisMapping(RedisMapping dbMapping) {
        this.redisMapping = dbMapping;
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
        if (redisMapping.keyType == null || redisMapping.keyType.isEmpty()) {
            throw new NullPointerException("redisMapping.keyType");
        }

        if (redisMapping.key == null || redisMapping.key.isEmpty()) {
            throw new NullPointerException("redisMapping.key");
        }

        if (redisMapping.value == null || redisMapping.value.isEmpty()) {
            throw new NullPointerException("redisMapping.value");
        }

        if (redisMapping.condition == null || redisMapping.condition.isEmpty()) {
            throw new NullPointerException("redisMapping.condition");
        }

    }


    public static class RedisMapping implements AdapterMapping {
        private String                  database;                                   // 数据库名或schema名
        private String                  table;
        private String keyType;
        private String key;
        private String value;
        private String condition;
        private String etlCondition;                        // etl条件sql

        @Override public String getEtlCondition() {
            return etlCondition;
        }

        public String getKeyType() {
            return keyType;
        }

        public void setKeyType(String keyType) {
            this.keyType = keyType;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getCondition() {
            return condition;
        }

        public void setCondition(String condition) {
            this.condition = condition;
        }

        public void setEtlCondition(String etlCondition) {
            this.etlCondition = etlCondition;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }
    }
}
