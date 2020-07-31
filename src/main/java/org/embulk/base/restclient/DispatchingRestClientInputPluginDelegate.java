package org.embulk.base.restclient;

import java.util.List;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;

public abstract class DispatchingRestClientInputPluginDelegate<T extends RestClientInputTaskBase>
        implements RestClientInputPluginDelegate<T> {
    public DispatchingRestClientInputPluginDelegate() {
        this.delegateSelected = null;
    }

    @Override  // Overridden from |InputTaskValidatable|
    public final void validateInputTask(final T task) {
        final RestClientInputPluginDelegate<T> delegate = this.cacheDelegate(task);
        delegate.validateInputTask(task);
    }

    @Override  // Overridden from |ServiceResponseMapperBuildable|
    public final ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(final T task) {
        final RestClientInputPluginDelegate<T> delegate = this.cacheDelegate(task);
        return delegate.buildServiceResponseMapper(task);
    }

    @Override  // Overridden from |ConfigDiffBuildable|
    public final ConfigDiff buildConfigDiff(
            final T task, final Schema schema, final int taskCount, final List<TaskReport> taskReports) {
        final RestClientInputPluginDelegate<T> delegate = this.cacheDelegate(task);
        return delegate.buildConfigDiff(task, schema, taskCount, taskReports);
    }

    @Override  // Overridden from |ServiceDataSplitterBuildable|
    public final ServiceDataSplitter<T> buildServiceDataSplitter(final T task) {
        final RestClientInputPluginDelegate<T> delegate = this.cacheDelegate(task);
        return delegate.buildServiceDataSplitter(task);
    }

    @Override  // Overridden from |ServiceDataIngestable|
    public final TaskReport ingestServiceData(
            final T task, final RecordImporter recordImporter, final int taskIndex, final PageBuilder pageBuilder) {
        final RestClientInputPluginDelegate<T> delegate = this.cacheDelegate(task);
        return delegate.ingestServiceData(task, recordImporter, taskIndex, pageBuilder);
    }

    /**
     * Returns an appropriate Delegate instance for the given Task.
     *
     * This method is to be overridden by the plugin Delegate class.
     */
    protected abstract RestClientInputPluginDelegate<T> dispatchPerTask(T task);

    private RestClientInputPluginDelegate<T> cacheDelegate(final T task) {
        if (this.delegateSelected == null) {
            this.delegateSelected = this.dispatchPerTask(task);
        }
        return this.delegateSelected;
    }

    private RestClientInputPluginDelegate<T> delegateSelected;
}
