package org.embulk.base.restclient.record;

abstract public class ServiceRecord<T extends ValueLocator>
{
    abstract public ServiceValue getValue(T locator);
}
