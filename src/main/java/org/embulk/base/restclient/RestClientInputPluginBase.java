package org.embulk.base.restclient;

import java.util.List;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

/**
 * RestClientInputPluginBase is a base class of input plugin implementations.
 *
 * Plugin methods of this {@code RestClientInputPluginBase} are all {@code final} to avoid confusion
 * caused by inappropriate overrides. Though {@code RestClientInputPluginBaseUnsafe} is available to
 * override the plugin methods, it is really discouraged. Contact the library authors when the base
 * class does not cover use cases.
 */
public class RestClientInputPluginBase<T extends RestClientInputTaskBase> extends RestClientInputPluginBaseUnsafe<T> {
    /**
     * Creates a new {@code RestClientInputPluginBase} instance.
     *
     * This constructor is designed to be called like {@code super(...);} as this class is to be inherited.
     */
    protected RestClientInputPluginBase(
            final Class<T> taskClass,
            final ConfigDiffBuildable<T> configDiffBuilder,
            final InputTaskValidatable<T> inputTaskValidator,
            final ServiceDataIngestable<T> serviceDataIngester,
            final ServiceDataSplitterBuildable<T> serviceDataSplitterBuilder,
            final ServiceResponseMapperBuildable<T> serviceResponseMapperBuilder) {
        super(taskClass,
              configDiffBuilder,
              inputTaskValidator,
              serviceDataIngester,
              serviceDataSplitterBuilder,
              serviceResponseMapperBuilder);
    }

    /**
     * Creates a new {@code RestClientInputPluginBase} instance.
     *
     * This constructor is designed to be called like {@code super(...);} as this class is to be inherited.
     */
    protected RestClientInputPluginBase(
            final Class<T> taskClass,
            final ConfigDiffBuildable<T> configDiffBuilder,
            final InputTaskValidatable<T> inputTaskValidator,
            final ServiceDataIngestable<T> serviceDataIngester,
            final ServiceResponseMapperBuildable<T> serviceResponseMapperBuilder) {
        super(taskClass,
              configDiffBuilder,
              inputTaskValidator,
              serviceDataIngester,
              serviceResponseMapperBuilder);
    }

    /**
     * Creates a new {@code RestClientInputPluginBase} instance.
     *
     * This constructor is designed to be called like {@code super(...);} as this class is to be inherited.
     */
    protected RestClientInputPluginBase(final Class<T> taskClass, final RestClientInputPluginDelegate<T> delegate) {
        super(taskClass, delegate, delegate, delegate, delegate, delegate);
    }

    @Override
    public final ConfigDiff transaction(final ConfigSource config, final InputPlugin.Control control) {
        return super.transaction(config, control);
    }

    @Override
    public final ConfigDiff resume(
            final TaskSource taskSource, final Schema schema, final int taskCount, final InputPlugin.Control control) {
        return super.resume(taskSource, schema, taskCount, control);
    }

    @Override
    public final void cleanup(
            final TaskSource taskSource, final Schema schema, final int taskCount, final List<TaskReport> successTaskReports) {
        super.cleanup(taskSource, schema, taskCount, successTaskReports);
    }

    @Override
    public final TaskReport run(final TaskSource taskSource, final Schema schema, final int taskIndex, final PageOutput output) {
        return super.run(taskSource, schema, taskIndex, output);
    }

    @Override
    public final ConfigDiff guess(final ConfigSource config) {
        return super.guess(config);
    }
}
