package org.embulk.base.restclient;

import java.util.List;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.spi.Schema;

public abstract class DispatchingRestClientOutputPluginDelegate<T extends RestClientOutputTaskBase>
        implements RestClientOutputPluginDelegate<T>
{
    public DispatchingRestClientOutputPluginDelegate()
    {
        this.delegateSelected = null;
    }

    @Override  // Overridden from |OutputTaskValidatable|
    public void validateOutputTask(T task, Schema embulkSchema, int taskCount)
    {
        final RestClientOutputPluginDelegate<T> delegate = this.cacheDelegate(task);
        delegate.validateOutputTask(task, embulkSchema, taskCount);
    }

    @Override  // Overridden from |ServiceRequestMapperBuildable|
    public ServiceRequestMapper<? extends ValueLocator> buildServiceRequestMapper(T task, Schema schema)
    {
        final RestClientOutputPluginDelegate<T> delegate = this.cacheDelegate(task);
        return delegate.buildServiceRequestMapper(task, schema);
    }

    @Override  // Overridden from |RecordBufferBuildable|
    public RecordBuffer buildRecordBuffer(T task, Schema schema, int taskIndex)
    {
        final RestClientOutputPluginDelegate<T> delegate = this.cacheDelegate(task);
        return delegate.buildRecordBuffer(task, schema, taskIndex);
    }

    @Override  // Overridden from |EmbulkDataEgestable|
    public final ConfigDiff egestEmbulkData(T task, Schema schema, int taskCount, List<TaskReport> taskReports)
    {
        final RestClientOutputPluginDelegate<T> delegate = this.cacheDelegate(task);
        return delegate.egestEmbulkData(task, schema, taskCount, taskReports);
    }

    /**
     * Returns an appropriate Delegate instance for the given Task.
     *
     * This method is to be overridden by the plugin Delegate class.
     */
    protected abstract RestClientOutputPluginDelegate<T> dispatchPerTask(T task);

    private RestClientOutputPluginDelegate<T> cacheDelegate(T task)
    {
        if (this.delegateSelected == null) {
            this.delegateSelected = dispatchPerTask(task);
        }
        return this.delegateSelected;
    }

    private RestClientOutputPluginDelegate<T> delegateSelected;
}
