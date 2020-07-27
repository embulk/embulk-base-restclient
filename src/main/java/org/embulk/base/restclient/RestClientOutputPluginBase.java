package org.embulk.base.restclient;

import java.util.List;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

/**
 * RestClientOutputPluginBase is a base class of output plugin implementations.
 *
 * Plugin methods of this {@code RestClientOutputPluginBase} are all {@code final} to avoid confusion
 * caused by inappropriate overrides. Though {@code RestClientOutputPluginBaseUnsafe} is available to
 * override the plugin methods, it is really discouraged. Contact the library authors when the base
 * class does not cover use cases.
 */
public class RestClientOutputPluginBase<T extends RestClientOutputTaskBase> extends RestClientOutputPluginBaseUnsafe<T> {
    /**
     * Creates a new {@code RestClientOutputPluginBase} instance.
     *
     * This constructor is designed to be called like {@code super(...);} as this class is to be inherited.
     */
    protected RestClientOutputPluginBase(
            final Class<T> taskClass,
            final EmbulkDataEgestable<T> embulkDataEgester,
            final RecordBufferBuildable<T> recordBufferBuilder,
            final OutputTaskValidatable<T> outputTaskValidator,
            final ServiceRequestMapperBuildable<T> serviceRequestMapperBuilder) {
        super(taskClass,
              embulkDataEgester,
              recordBufferBuilder,
              outputTaskValidator,
              serviceRequestMapperBuilder);
    }

    /**
     * Creates a new {@code RestClientOutputPluginBase} instance.
     *
     * This constructor is designed to be called like {@code super(...);} as this class is to be inherited.
     */
    protected RestClientOutputPluginBase(final Class<T> taskClass, final RestClientOutputPluginDelegate<T> delegate) {
        super(taskClass, delegate, delegate, delegate, delegate);
    }

    @Override
    public final ConfigDiff transaction(
            final ConfigSource config, final Schema schema, final int taskCount, final OutputPlugin.Control control) {
        return super.transaction(config, schema, taskCount, control);
    }

    @Override
    public final ConfigDiff resume(
            final TaskSource taskSource, final Schema schema, final int taskCount, final OutputPlugin.Control control) {
        return super.resume(taskSource, schema, taskCount, control);
    }

    @Override
    public final void cleanup(
            final TaskSource taskSource, final Schema schema, final int taskCount, final List<TaskReport> successTaskReports) {
        super.cleanup(taskSource, schema, taskCount, successTaskReports);
    }

    @Override
    public final TransactionalPageOutput open(final TaskSource taskSource, final Schema schema, final int taskIndex) {
        return super.open(taskSource, schema, taskIndex);
    }
}
