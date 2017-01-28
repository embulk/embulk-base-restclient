package org.embulk.base.restclient;

public interface ServiceResponseSchemaBuildable<T extends RestClientInputTaskBase>
{
    public ServiceResponseSchema buildServiceResponseSchema(T task);
}
