package org.embulk.base.restclient;

import org.embulk.base.restclient.record.ValueLocator;

public interface RestClientInputPluginDelegate<T extends RestClientInputTaskBase, U extends ValueLocator, V>
        extends ClientCreatable<T>,
                ConfigDiffBuildable<T>,
                ResponseReadable<V>,
                PageLoadable<T,U,V>,
                ServiceResponseSchemaBuildable<U>,
                TaskReportBuildable<T>,
                TaskValidatable<T>
{
}
