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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.config.RedisConnectorOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.connectors.redis.util.RedisTestBase;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.*;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_CLUSTER;
import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/** IT cases for Redis connector (including source and sink). */
public class RedisConnectorITCase extends RedisTestBase {
    
    private static Logger log = Logger.getLogger("logger");
    // -------------------------------------------------------------------------------------
    // Redis lookup source tests
    // -------------------------------------------------------------------------------------

    // prepare a source collection.
    private static final List<Row> testData = new ArrayList<>();
    private static final RowTypeInfo testTypeInfo =
            new RowTypeInfo(
                    new TypeInformation[] {Types.INT, Types.LONG, Types.STRING},
                    new String[] {"a", "b", "c"});

    static {
        testData.add(Row.of(1, 1L, "xiamen"));
        testData.add(Row.of(2, 2L, "beijing"));
        testData.add(Row.of(3, 2L, "fuzhou"));
        testData.add(Row.of(3, 3L, "guangzhou"));
    }

    @Test
    public void testKVSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings
            .newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build();
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = String.format("create table sink_redis(username VARCHAR, passport VARCHAR) "
                                   + "with ( 'connector'='redis', "
                                   + "'host'='%s','port'='%s', "
                                   + "'redis-mode'='single',"
                                   + "'key-column'='username',"
                                   + "'value-column'='passport', '"
                                   + REDIS_COMMAND + "'='"
                                   + RedisCommand.SET + "')",
                                   REDIS_HOST, REDIS_PORT);
        log.info(ddl);

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test-key', 'test-value'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);
    }


    @Test
    public void testTableSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
            EnvironmentSettings
            .newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build();
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = String.format("create table sink_redis(username VARCHAR, level VARCHAR, age VARCHAR) "
                                   + "with ( 'connector'='redis', "
                                   + "'host'='%s','port'='%s', "
                                   + "'redis-mode'='single',"
                                   + "'field-column'='level',"
                                   + "'key-column'='username', 'put-if-absent'='true','value-column'='age', '"
                                   + REDIS_COMMAND + "'='"
                                   + RedisCommand.HSET + "', "
                                   + "'maxIdle'='2', 'minIdle'='1'  )",
                                   REDIS_HOST, REDIS_PORT) ;

        log.info(ddl);

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test_hash', '3', '15'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);
    }

    @Test
    public void testRedisLookupJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings =
            EnvironmentSettings
            .newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build();
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        // LOOKUP TABLE
        String lookup_ddl = String.format("create table lookup_redis(field VARCHAR, v VARCHAR) with ("
                                   + "'connector'='redis', "
                                   + "'host'='%s','port'='%s', "
                                   + "'redis-mode'='single',"
                                   + "'lookup.additional-key' = '"
                                   + TEST_HGET_KEY + "',"
                                   + "'lookup.cache.max-rows' = '1000',"
                                   + "'lookup.cache.ttl-sec' = '600',"
                                   + "'" + REDIS_COMMAND + "'='"
                                   + RedisCommand.HGET + "',"
                                   + " 'maxIdle'='2', 'minIdle'='1'  )",
                                   REDIS_HOST, REDIS_PORT) ;

        log.info(lookup_ddl);
        tEnv.executeSql(lookup_ddl);

        // prepare a source table
        String srcTableName = "src";
        DataStream<Row> srcDs = env.fromCollection(testData).returns(testTypeInfo);
        Table in = tEnv.fromDataStream(srcDs, $("a"), $("b"), $("c"), $("proc").proctime());
        tEnv.createTemporaryView(srcTableName, in);

        // perform a temporal table join query
        String dimJoinQuery =
            "SELECT"
            + " a,"
            + " b,"
            + " h.v"
            + " FROM src JOIN "
            + "lookup_redis"
            + " FOR SYSTEM_TIME AS OF src.proc as h ON src.c = h.field";
        
        Iterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> result =
                Lists.newArrayList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        log.info(result.toString());
        List<String> expected = new ArrayList<>();
        expected.add("+I[1, 1, " + TEST_HGET_VALUE + "]");
        expected.add("+I[2, 2, null]");
        expected.add("+I[3, 2, null]");
        expected.add("+I[3, 3, null]");
        assertEquals(expected, result);

    }


}
