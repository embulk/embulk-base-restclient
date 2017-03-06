package org.embulk.base.restclient.jackson.scope;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.embulk.base.restclient.record.SinglePageRecordReader;

public class JacksonNewArrayScope
        extends JacksonArrayScopeBase
{
    @Override
    public ArrayNode scopeArray(SinglePageRecordReader singlePageRecordReader)
    {
        return JsonNodeFactory.instance.arrayNode();
    }
}
