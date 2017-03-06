package org.embulk.base.restclient.jackson.scope;

import com.fasterxml.jackson.databind.node.LongNode;

import org.embulk.base.restclient.jackson.JacksonServiceValue;
import org.embulk.base.restclient.record.EmbulkValueScope;
import org.embulk.base.restclient.record.SinglePageRecordReader;

public abstract class JacksonIntegerScopeBase
        extends EmbulkValueScope
{
    public abstract long scopeInteger(SinglePageRecordReader singlePageRecordReader);

    @Override
    public final JacksonServiceValue scopeEmbulkValues(SinglePageRecordReader singlePageRecordReader)
    {
        return new JacksonServiceValue(new LongNode(this.scopeInteger(singlePageRecordReader)));
    }
}
