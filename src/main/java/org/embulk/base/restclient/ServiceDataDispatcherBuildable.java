package org.embulk.base.restclient;

public interface ServiceDataDispatcherBuildable<T extends RestClientInputTaskBase>
{
    public ServiceDataDispatcher buildServiceDataDispatcher(T task);
}
