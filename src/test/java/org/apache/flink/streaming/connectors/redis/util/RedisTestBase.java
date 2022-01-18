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


package org.apache.flink.streaming.connectors.redis.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;



public abstract class RedisTestBase {
    protected static final String REDIS_HOST = "localhost";
    protected static final String REDIS_PORT = "6379";
    protected static final String TEST_HGET_KEY = "test_city_info";
    protected static final String TEST_HGET_FIELD = "xiamen";
    protected static final String TEST_HGET_VALUE = "ok";
    protected static final String TEST_GET_KEY = "xiamen";
    protected static final String TEST_GET_VALUE = "ok";
    protected static JedisPool jedisPool = null;


    @Before
    public void before() throws IOException {
        initJedisPool();
        createRedisHGetKey();
    }

    private static void initJedisPool() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        jedisPool = new JedisPool(poolConfig, REDIS_HOST);
    }

    private static void createRedisHGetKey() throws IOException {
        Jedis jedis = jedisPool.getResource();
        jedis.hset(TEST_HGET_KEY, TEST_HGET_FIELD, TEST_HGET_VALUE);
        jedis.set(TEST_GET_KEY, TEST_GET_VALUE);
        jedis.close();
    }
}
