package org.embulk.base.restclient;

public interface RestClientOutputPluginDelegate<T extends RestClientOutputTaskBase>
        extends EmbulkDataEgestable<T>,
                RecordBufferBuildable<T>,
                ServiceRequestMapperBuildable<T>,
                TaskValidatable<T>
{
}
