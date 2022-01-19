/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import java.util.HashSet;
import java.util.Set;
import java.util.Map;

import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.HOST;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.PORT;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.DATABASE;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.MAXIDLE;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.MAXTOTAL;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.CLUSTERNODES;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.PASSWORD;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.TIMEOUT;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.MINIDLE;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.COMMAND;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.REDISMODE;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.KEY_COLUMN;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.VALUE_COLUMN;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.FIELD_COLUMN;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.WILDCARD_COLUMN;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.PUT_IF_ABSENT;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.TTL;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.LOOKUP_ADDITIONAL_KEY;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.LOOKUP_CACHE_TTL_SEC;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

/**
 * Factory for creating configured instances of RedisDynamicSource and
 * RedisDynamicSink
 */

public class RedisDynamicTableFactory
    implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "redis";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        options.add(PORT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DATABASE);
        options.add(MAXIDLE);
        options.add(MAXTOTAL);
        options.add(CLUSTERNODES);
        options.add(PASSWORD);
        options.add(TIMEOUT);
        options.add(MINIDLE);
        options.add(COMMAND);
        options.add(REDISMODE);
        options.add(KEY_COLUMN);
        options.add(VALUE_COLUMN);
        options.add(FIELD_COLUMN);
        options.add(WILDCARD_COLUMN);
        options.add(PUT_IF_ABSENT);
        options.add(TTL);
        options.add(LOOKUP_ADDITIONAL_KEY);
        options.add(LOOKUP_CACHE_MAX_ROWS);
        options.add(LOOKUP_CACHE_TTL_SEC);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        TableSchema schema = context.getCatalogTable().getSchema();
        validateConfigOptions(config);
        return createRedisTableSource(config, schema, context.getCatalogTable().getOptions());

    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        if(context.getCatalogTable().getOptions().containsKey(REDIS_COMMAND)){
            context.getCatalogTable().getOptions().put(REDIS_COMMAND, context.getCatalogTable().getOptions().get(REDIS_COMMAND).toUpperCase());
        }
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);
        return new RedisDynamicTableSink(context.getCatalogTable().getOptions(), context.getCatalogTable().getSchema(), config);
    }

    // --------------------------------------------------------------------------------------------

    protected RedisDynamicTableSource createRedisTableSource(ReadableConfig config, TableSchema schema, Map<String, String> properties) {
        return new RedisDynamicTableSource(config, schema, properties);
    }

    private void validateConfigOptions(ReadableConfig config){

    }

}
