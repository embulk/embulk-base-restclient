package org.embulk.base.restclient;

public interface RestClientInputPluginDelegate<T extends RestClientInputTaskBase>
        extends ClientCreatable<T>,
                ConfigDiffBuildable<T>,
                RetryConfigurable<T>,
                ServiceDataIngestable<T>,
                ServiceResponseMapperBuildable<T>,
                TaskValidatable<T>
{
}
