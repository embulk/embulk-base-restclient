package org.embulk.base.restclient.jackson.scope;

import org.embulk.base.restclient.record.SinglePageRecordReader;

public class JacksonImmediateIntegerScope
        extends JacksonIntegerScopeBase
{
    public JacksonImmediateIntegerScope(long immediateInteger)
    {
        this.immediateInteger = immediateInteger;
    }

    @Override
    public long scopeInteger(SinglePageRecordReader singlePageRecordReader)
    {
        return this.immediateInteger;
    }

    private final long immediateInteger;
}
