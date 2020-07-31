package org.embulk.base.restclient;

public interface ServiceDataSplitterBuildable<T extends RestClientInputTaskBase> {
    ServiceDataSplitter<T> buildServiceDataSplitter(T task);
}
