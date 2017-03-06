package org.embulk.base.restclient.jackson;

import com.fasterxml.jackson.databind.node.TextNode;

import org.embulk.base.restclient.record.EmbulkValueScope;
import org.embulk.base.restclient.record.SinglePageRecordReader;

public abstract class JacksonStringValueScope
        extends EmbulkValueScope
{
    public abstract String scopeStringValue(SinglePageRecordReader singlePageRecordReader);

    @Override
    public final JacksonServiceValue scopeEmbulkValues(SinglePageRecordReader singlePageRecordReader)
    {
        return new JacksonServiceValue(new TextNode(this.scopeStringValue(singlePageRecordReader)));
    }
}
