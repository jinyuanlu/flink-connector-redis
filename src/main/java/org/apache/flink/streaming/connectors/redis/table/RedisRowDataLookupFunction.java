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

import org.apache.flink.util.Preconditions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.util.Preconditions;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Cache;

import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.COMMAND;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.LOOKUP_ADDITIONAL_KEY;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions.LOOKUP_CACHE_TTL_SEC;


public class RedisRowDataLookupFunction extends TableFunction<RowData> {
    private static final long serialVersionUID = 42L;

    private final ReadableConfig options;
    private final String command;
    private final String additionalKey;
    private final int cacheMaxRows;
    private final int cacheTtlSec;
    private final Map<String, String> properties;
    private RedisCommandsContainer commandsContainer;
    private transient Cache<RowData, RowData> cache;

    public RedisRowDataLookupFunction(Map<String, String> properties, ReadableConfig options) {
      Preconditions.checkNotNull(options, "No options supplied");
      this.options = options;

      command = options.get(COMMAND).toUpperCase();
      Preconditions
          .checkArgument(
                         command.equals("GET") || command.equals("HGET"),
                         "Redis table source only supports GET and HGET commands"
                         );

      additionalKey = options.get(LOOKUP_ADDITIONAL_KEY);
      cacheMaxRows = options.get(LOOKUP_CACHE_MAX_ROWS);
      cacheTtlSec = options.get(LOOKUP_CACHE_TTL_SEC);
      this.properties = properties;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
      super.open(context);

      FlinkJedisConfigBase jedisConfig = RedisHandlerServices
           .findRedisHandler(
                             FlinkJedisConfigHandler.class,
                             this.properties
                             )
           .createFlinkJedisConfig(options);

      commandsContainer = RedisCommandsContainerBuilder.build(jedisConfig);
      commandsContainer.open();

      if (cacheMaxRows > 0 && cacheTtlSec > 0) {
        cache = CacheBuilder.newBuilder()
          .expireAfterWrite(cacheTtlSec, TimeUnit.SECONDS)
          .maximumSize(cacheMaxRows)
          .build();
      }
    }

    @Override
    public void close() throws Exception {
      if (cache != null) {
        cache.invalidateAll();
      }
      if (commandsContainer != null) {
        commandsContainer.close();
      }
      super.close();
    }

    public void eval(Object obj) {
      RowData lookupKey = GenericRowData.of(obj);
      if (cache != null) {
        RowData cachedRow = cache.getIfPresent(lookupKey);
        if (cachedRow != null) {
          collect(cachedRow);
          return;
        }
      }

      StringData key = lookupKey.getString(0);
      String value = command.equals("GET") ? commandsContainer.get(key.toString()) : commandsContainer.hget(additionalKey, key.toString());
      RowData result = GenericRowData.of(key, StringData.fromString(value));

      cache.put(lookupKey, result);
      collect(result);
    }
  }
