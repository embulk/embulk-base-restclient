package org.embulk.base.restclient;

import org.embulk.base.restclient.record.ValueLocator;

public interface ServiceResponseMapperBuildable<T extends RestClientInputTaskBase>
{
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(T task);
}
