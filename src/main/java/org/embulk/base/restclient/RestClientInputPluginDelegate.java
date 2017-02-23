package org.embulk.base.restclient;

public interface RestClientInputPluginDelegate<T extends RestClientInputTaskBase>
        extends ConfigDiffBuildable<T>,
                ServiceDataIngestable<T>,
                ServiceResponseMapperBuildable<T>,
                TaskValidatable<T>
{
}
