package org.embulk.base.restclient;

import org.embulk.base.restclient.ServiceResponseSchema;
import org.embulk.base.restclient.record.ValueLocator;

public interface ServiceResponseSchemaBuildable<T extends ValueLocator>
{
    public ServiceResponseSchema<T> buildServiceResponseSchema();
}
