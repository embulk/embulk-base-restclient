package org.embulk.base.restclient.jackson.scope;

import org.embulk.base.restclient.record.SinglePageRecordReader;

public class JacksonImmediateStringScope
        extends JacksonStringScopeBase
{
    public JacksonImmediateStringScope(String immediateString)
    {
        this.immediateString = immediateString;
    }

    @Override
    public String scopeString(SinglePageRecordReader singlePageRecordReader)
    {
        return this.immediateString;
    }

    private final String immediateString;
}
