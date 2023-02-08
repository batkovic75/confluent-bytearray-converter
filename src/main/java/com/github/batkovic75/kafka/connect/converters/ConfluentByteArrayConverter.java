/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.batkovic75.kafka.connect.converters;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.util.Arrays;
import java.util.Map;

public class ConfluentByteArrayConverter implements Converter, HeaderConverter {
    static final String FIELD_CONFIG = "bytes.count";
    private static final ConfigDef CONFIG_DEF =
        new ConfigDef()
            .define(
                    FIELD_CONFIG,
                    ConfigDef.Type.INT,
                    6,
                    ConfigDef.Importance.HIGH,
                    "The value of bytes that needs to be stripped from beginning of the byte array."
            );
    private static final String PURPOSE = "Access field to check if it's null";

    private int bytesCount;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        configure(configs, false);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);
        bytesCount = config.getInt(FIELD_CONFIG);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema != null && schema.type() != Schema.Type.BYTES)
            throw new DataException("Invalid schema type for ByteArrayConverter: " + schema.type().toString());

        if (value != null && !(value instanceof byte[]))
            throw new DataException("ByteArrayConverter is not compatible with objects of type " + value.getClass());

        byte[] bytes = (byte[]) value;

        return Arrays.copyOfRange(bytes, bytesCount, bytes.length);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        byte[] truncatedBytes = Arrays.copyOfRange(value, bytesCount, value.length);
        return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, truncatedBytes);
    }

    @Override
    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        return fromConnectData(topic, schema, value);
    }

    @Override
    public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
        return toConnectData(topic, value);
    }

    @Override
    public void close() {
        // do nothing
    }
}