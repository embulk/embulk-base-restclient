package org.embulk.base.restclient.jackson;

import com.fasterxml.jackson.databind.node.LongNode;

import org.embulk.base.restclient.record.EmbulkValueScope;
import org.embulk.base.restclient.record.SinglePageRecordReader;

public abstract class JacksonIntegerValueScope
        extends EmbulkValueScope
{
    public abstract long scopeIntegerValue(SinglePageRecordReader singlePageRecordReader);

    @Override
    public final JacksonServiceValue scopeEmbulkValues(SinglePageRecordReader singlePageRecordReader)
    {
        return new JacksonServiceValue(new LongNode(this.scopeIntegerValue(singlePageRecordReader)));
    }
}
