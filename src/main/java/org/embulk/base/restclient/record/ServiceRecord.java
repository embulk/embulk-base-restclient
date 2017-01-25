package org.embulk.base.restclient.record;

public abstract class ServiceRecord
{
    public abstract ServiceValue getValue(ValueLocator locator);
}
