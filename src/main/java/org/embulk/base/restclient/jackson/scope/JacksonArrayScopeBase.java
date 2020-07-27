package org.embulk.base.restclient.jackson.scope;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.embulk.base.restclient.jackson.JacksonServiceValue;
import org.embulk.base.restclient.record.EmbulkValueScope;
import org.embulk.base.restclient.record.SinglePageRecordReader;

public abstract class JacksonArrayScopeBase extends EmbulkValueScope {
    public abstract ArrayNode scopeArray(SinglePageRecordReader singlePageRecordReader);

    @Override
    public final JacksonServiceValue scopeEmbulkValues(final SinglePageRecordReader singlePageRecordReader) {
        return new JacksonServiceValue(this.scopeArray(singlePageRecordReader));
    }
}
