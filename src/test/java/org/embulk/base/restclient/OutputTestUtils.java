package org.embulk.base.restclient;

import com.google.common.collect.ImmutableMap;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

class OutputTestUtils {

    private static String JSON_PATH_PREFIX;

    void initializeConstant()
    {
        //noinspection ConstantConditions
        JSON_PATH_PREFIX = OutputTestUtils.class.getClassLoader().getResource("sample_01.json").getPath();
    }

    ConfigSource configJSON()
    {
        return Exec.newConfigSource()
                .set("in", inputConfigJSON())
                .set("parser", parserConfigJSON());
    }

    private ImmutableMap<String, Object> inputConfigJSON()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", JSON_PATH_PREFIX);
        builder.put("last_path", "");
        return builder.build();
    }

    private ImmutableMap<String, Object> parserConfigJSON()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        return builder.build();
    }

    Schema JSONSchema()
    {
        return Schema.builder()
                .add("id", Types.LONG)
                .add("long", Types.LONG)
                .add("timestamp", Types.TIMESTAMP)
                .add("boolean", Types.BOOLEAN)
                .add("double", Types.DOUBLE)
                .add("string", Types.STRING)
                .build();
    }

}