package org.embulk.base.restclient;

public interface RestClientOutputPluginDelegate<T extends RestClientOutputTaskBase>
        extends EmbulkDataEgestable<T>,
                RecordBufferBuildable<T>,
                OutputTaskValidatable<T>,
                ServiceRequestMapperBuildable<T> {
}
