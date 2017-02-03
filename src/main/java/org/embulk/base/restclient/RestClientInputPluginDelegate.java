package org.embulk.base.restclient;

import org.embulk.base.restclient.record.ValueLocator;

public interface RestClientInputPluginDelegate<T extends RestClientInputTaskBase>
        extends ClientCreatable<T>,
                ConfigDiffBuildable<T>,
                ServiceDataIngestable<T>,
                ServiceResponseMapperBuildable<T>,
                TaskValidatable<T>
{
}
