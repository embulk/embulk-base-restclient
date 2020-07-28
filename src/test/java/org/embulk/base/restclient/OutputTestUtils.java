/*
 * Copyright 2016 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.base.restclient;

import com.google.common.collect.ImmutableMap;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

class OutputTestUtils {
    void initializeConstant() {
        // noinspection ConstantConditions
        JSON_PATH_PREFIX = OutputTestUtils.class.getClassLoader().getResource("sample_01.json").getPath();
    }

    ConfigSource configJson() {
        return Exec.newConfigSource()
                .set("in", inputConfigJson())
                .set("parser", parserConfigJson());
    }

    private ImmutableMap<String, Object> inputConfigJson() {
        final ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", JSON_PATH_PREFIX);
        builder.put("last_path", "");
        return builder.build();
    }

    private ImmutableMap<String, Object> parserConfigJson() {
        final ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        return builder.build();
    }

    Schema jsonSchema() {
        return Schema.builder()
                .add("id", Types.LONG)
                .add("long", Types.LONG)
                .add("timestamp", Types.TIMESTAMP)
                .add("boolean", Types.BOOLEAN)
                .add("double", Types.DOUBLE)
                .add("string", Types.STRING)
                .build();
    }

    private static String JSON_PATH_PREFIX;
}
