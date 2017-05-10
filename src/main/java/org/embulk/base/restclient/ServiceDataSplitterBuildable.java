package org.embulk.base.restclient;

public interface ServiceDataSplitterBuildable<T extends RestClientInputTaskBase>
{
    public ServiceDataSplitter<T> buildServiceDataSplitter(T task);
}
