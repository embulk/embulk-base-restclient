package org.embulk.base.restclient.jackson.scope;

import org.embulk.base.restclient.record.SinglePageRecordReader;

public class JacksonImmediateStringScope extends JacksonStringScopeBase {
    public JacksonImmediateStringScope(final String immediateString) {
        this.immediateString = immediateString;
    }

    @Override
    public String scopeString(final SinglePageRecordReader singlePageRecordReader) {
        return this.immediateString;
    }

    private final String immediateString;
}
