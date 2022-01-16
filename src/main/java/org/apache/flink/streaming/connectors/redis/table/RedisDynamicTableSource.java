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
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.streaming.connectors.redis.table.RedisRowDataLookupFunction;

import java.util.Map;


public class RedisDynamicTableSource implements LookupTableSource {
    private final ReadableConfig options;
    private final TableSchema schema;
    private final Map<String, String> properties;


    public RedisDynamicTableSource(ReadableConfig options, TableSchema schema, Map<String, String> properties) {
        this.options = options;
        this.schema = schema;
        this.properties = properties;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {

        DataType[] dataTypes = schema.getFieldDataTypes();

        return TableFunctionProvider.of(new RedisRowDataLookupFunction(this.properties, options));
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(options, schema, properties);
    }

    @Override
    public String asSummaryString() {
        return "Redis Dynamic Table Source";
    }

}
