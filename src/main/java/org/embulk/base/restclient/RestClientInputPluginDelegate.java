package org.embulk.base.restclient;

import org.embulk.base.restclient.record.ValueLocator;

public interface RestClientInputPluginDelegate<T extends RestClientInputTaskBase, U extends ValueLocator>
        extends ClientCreatable<T>,
                ConfigDiffBuildable<T>,
                PageLoadable<T,U>,
                ServiceResponseSchemaBuildable<U>,
                TaskReportBuildable<T>,
                TaskValidatable<T>
{
}
