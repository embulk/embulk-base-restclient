package org.embulk.base.restclient.record;

public abstract class ServiceRecord<T extends ValueLocator>
{
    public abstract ServiceValue getValue(T locator);
}
