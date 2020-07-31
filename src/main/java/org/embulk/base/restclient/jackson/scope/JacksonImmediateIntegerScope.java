package org.embulk.base.restclient.jackson.scope;

import org.embulk.base.restclient.record.SinglePageRecordReader;

public class JacksonImmediateIntegerScope extends JacksonIntegerScopeBase {
    public JacksonImmediateIntegerScope(final long immediateInteger) {
        this.immediateInteger = immediateInteger;
    }

    @Override
    public long scopeInteger(final SinglePageRecordReader singlePageRecordReader) {
        return this.immediateInteger;
    }

    private final long immediateInteger;
}
