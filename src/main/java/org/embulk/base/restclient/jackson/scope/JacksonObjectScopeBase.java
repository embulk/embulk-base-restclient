package org.embulk.base.restclient.jackson.scope;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.base.restclient.jackson.JacksonServiceValue;
import org.embulk.base.restclient.record.EmbulkValueScope;
import org.embulk.base.restclient.record.SinglePageRecordReader;

public abstract class JacksonObjectScopeBase extends EmbulkValueScope {
    public abstract ObjectNode scopeObject(SinglePageRecordReader singlePageRecordReader);

    @Override
    public final JacksonServiceValue scopeEmbulkValues(final SinglePageRecordReader singlePageRecordReader) {
        return new JacksonServiceValue(this.scopeObject(singlePageRecordReader));
    }
}
