package org.embulk.base.restclient;

public interface ServiceResponseMapperBuildable<T extends RestClientInputTaskBase>
{
    public ServiceResponseMapper buildServiceResponseMapper(T task);
}
