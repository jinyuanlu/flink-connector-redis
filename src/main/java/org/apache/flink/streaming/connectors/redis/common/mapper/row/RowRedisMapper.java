package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.data.GenericRowData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

/**
 * base row redis mapper implement.
 */
public abstract class RowRedisMapper implements RedisMapper<GenericRowData>, RedisMapperHandler {

    private Integer ttl;

    private RedisCommand redisCommand;

    private String fieldColumn;

    private String keyColumn;

    private String valueColumn;

    private boolean putIfAbsent;

    private String additionalKey;

    private Integer cacheMaxRows;

    private Integer cacheTtlSec;

    public RowRedisMapper(int ttl, RedisCommand redisCommand, String keyColumn, String valueColumn, boolean putIfAbsent) {
        this.ttl = ttl;
        this.redisCommand = redisCommand;
        this.keyColumn = keyColumn;
        this.valueColumn = valueColumn;
        this.putIfAbsent = putIfAbsent;
    }

    public RowRedisMapper(RedisCommand redisCommand,  String keyColumn, String fieldColumn, String valueColumn, boolean putIfAbsent, int ttl){
        this.ttl = ttl;
        this.redisCommand = redisCommand;
        this.keyColumn = keyColumn;
        this.fieldColumn = fieldColumn;
        this.valueColumn = valueColumn;
        this.putIfAbsent = putIfAbsent;
    }

    public RowRedisMapper(RedisCommand redisCommand){
        this.redisCommand = redisCommand;
    }

    public RowRedisMapper(RedisCommand redisCommand, Map<String, String> config){
        this.redisCommand = redisCommand;
    }

    public RowRedisMapper(RedisCommand redisCommand, ReadableConfig config){
        this.redisCommand = redisCommand;
        this.ttl = config.get(RedisConnectorOptions.TTL);
        this.valueColumn = config.get(RedisConnectorOptions.VALUE_COLUMN);
        this.keyColumn = config.get(RedisConnectorOptions.KEY_COLUMN);
        this.fieldColumn = config.get(RedisConnectorOptions.FIELD_COLUMN);
        this.putIfAbsent = config.get(RedisConnectorOptions.PUT_IF_ABSENT);
        this.additionalKey = config.get(RedisConnectorOptions.LOOKUP_ADDITIONAL_KEY);
        this.cacheMaxRows = config.get(RedisConnectorOptions.LOOKUP_CACHE_MAX_ROWS);
        this.cacheTtlSec = config.get(RedisConnectorOptions.LOOKUP_CACHE_TTL_SEC);
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(
                                           redisCommand,
                                           ttl,
                                           keyColumn,
                                           fieldColumn,
                                           valueColumn,
                                           putIfAbsent,
                                           additionalKey,
                                           cacheMaxRows,
                                           cacheTtlSec
                                           );
    }

    @Override
    public String getKeyFromData(GenericRowData row, Integer keyIndex) {
        return String.valueOf(row.getField(keyIndex));
    }

    @Override
    public String getValueFromData(GenericRowData row,  Integer valueIndex) {
        return String.valueOf(row.getField(valueIndex));
    }

    @Override
    public String getFieldFromData(GenericRowData row, Integer fieldIndex) {
        return String.valueOf(row.getField(fieldIndex));
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_COMMAND, getRedisCommand().name());
        return require;
    }

    @Override
    public boolean equals(Object obj) {
        RedisCommand redisCommand = ((RowRedisMapper) obj).redisCommand;
        return this.redisCommand == redisCommand;
    }

}
