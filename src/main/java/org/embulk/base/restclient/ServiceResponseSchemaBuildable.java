package org.embulk.base.restclient;

import org.embulk.base.restclient.record.ValueLocator;

public interface ServiceResponseSchemaBuildable<T extends RestClientInputTaskBase>
{
    public ServiceResponseSchema<? extends ValueLocator> buildServiceResponseSchema(T task);
}
