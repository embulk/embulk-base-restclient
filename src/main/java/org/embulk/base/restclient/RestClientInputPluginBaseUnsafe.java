/*
 * Copyright 2017 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.base.restclient;

import java.util.List;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.util.config.ConfigMapperFactory;

/**
 * RestClientInputPluginBaseUnsafe is an "unsafe" base class of input plugin implementations.
 *
 * Do not use this {@code RestClientInputPluginBaseUnsafe} directly unless you really need this.
 * For almost all cases, inherit {@code RestClientInputPluginBase} instead.
 *
 * This {@code RestClientInputPluginBaseUnsafe} is here to allow overriding the plugin methods
 * such as {@code transaction} and {@code resume}. Overriding them can cause unexpected behavior.
 * Plugins using this {@code RestClientInputPluginBaseUnsafe} may have difficulties to catch up
 * with updates of this library.
 *
 * Use {@code RestClientInputPluginBase} instead of {@code RestClientInputPluginBaseUnsafe}.
 * Plugin methods of {@code RestClientInputPluginBase} are all {@code final} not to be overridden.
 */
public class RestClientInputPluginBaseUnsafe<T extends RestClientInputTaskBase>
        extends RestClientPluginBase<T>
        implements InputPlugin {
    protected RestClientInputPluginBaseUnsafe(
            final ConfigMapperFactory configMapperFactory,
            final Class<T> taskClass,
            final ConfigDiffBuildable<T> configDiffBuilder,
            final InputTaskValidatable<T> inputTaskValidator,
            final ServiceDataIngestable<T> serviceDataIngester,
            final ServiceDataSplitterBuildable<T> serviceDataSplitterBuilder,
            final ServiceResponseMapperBuildable<T> serviceResponseMapperBuilder) {
        super(configMapperFactory);
        this.taskClass = taskClass;
        this.configDiffBuilder = configDiffBuilder;
        this.inputTaskValidator = inputTaskValidator;
        this.serviceDataIngester = serviceDataIngester;
        this.serviceDataSplitterBuilder = serviceDataSplitterBuilder;
        this.serviceResponseMapperBuilder = serviceResponseMapperBuilder;
    }

    protected RestClientInputPluginBaseUnsafe(
            final ConfigMapperFactory configMapperFactory,
            final Class<T> taskClass,
            final ConfigDiffBuildable<T> configDiffBuilder,
            final InputTaskValidatable<T> inputTaskValidator,
            final ServiceDataIngestable<T> serviceDataIngester,
            final ServiceResponseMapperBuildable<T> serviceResponseMapperBuilder) {
        this(
                configMapperFactory,
                taskClass,
                configDiffBuilder,
                inputTaskValidator,
                serviceDataIngester,
                new ServiceDataSplitterBuildable<T>() {
                    @Override
                    public ServiceDataSplitter<T> buildServiceDataSplitter(final T task) {
                        return new DefaultServiceDataSplitter<T>();
                    }
                },
                serviceResponseMapperBuilder);
    }

    protected RestClientInputPluginBaseUnsafe(
            final ConfigMapperFactory configMapperFactory,
            final Class<T> taskClass,
            final RestClientInputPluginDelegate<T> delegate) {
        this(configMapperFactory, taskClass, delegate, delegate, delegate, delegate, delegate);
    }

    @Override
    public ConfigDiff transaction(final ConfigSource config, final InputPlugin.Control control) {
        final T task = loadConfig(config, this.taskClass);
        this.inputTaskValidator.validateInputTask(task);

        final int taskCount = this.serviceDataSplitterBuilder
                .buildServiceDataSplitter(task)
                .numberToSplitWithHintingInTask(task);

        final Schema schema = this.serviceResponseMapperBuilder.buildServiceResponseMapper(task).getEmbulkSchema();
        return resume(task.toTaskSource(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(
            final TaskSource taskSource, final Schema schema, final int taskCount, final InputPlugin.Control control) {
        final T task = loadTask(taskSource, this.taskClass);
        final List<TaskReport> taskReports = control.run(taskSource, schema, taskCount);
        return this.configDiffBuilder.buildConfigDiff(task, schema, taskCount, taskReports);
    }

    @Override
    public void cleanup(
            final TaskSource taskSource, final Schema schema, final int taskCount, final List<TaskReport> successTaskReports) {
    }

    @Override
    public TaskReport run(final TaskSource taskSource, final Schema schema, final int taskIndex, final PageOutput output) {
        final T task = loadTask(taskSource, this.taskClass);
        this.serviceDataSplitterBuilder
            .buildServiceDataSplitter(task)
            .hintInEachSplitTask(task, schema, taskIndex);

        final ServiceResponseMapper<? extends ValueLocator> serviceResponseMapper =
                this.serviceResponseMapperBuilder.buildServiceResponseMapper(task);

        try (final PageBuilder pageBuilder = getPageBuilder(Exec.getBufferAllocator(), schema, output)) {
            // When failing around |PageBuidler| in |ingestServiceData|, |pageBuilder.finish()| should not be called.
            final TaskReport taskReport = this.serviceDataIngester.ingestServiceData(
                    task, serviceResponseMapper.createRecordImporter(), taskIndex, pageBuilder);
            if (taskReport == null) {
                throw new NullPointerException("TaskReport is unexpectedly null.");
            }
            pageBuilder.finish();
            return taskReport;
        }
    }

    @Override
    public ConfigDiff guess(final ConfigSource config) {
        return this.newConfigDiff();
    }

    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk-base-restclient/issues/132
    private static PageBuilder getPageBuilder(final BufferAllocator bufferAllocator, final Schema schema, final PageOutput output) {
        if (HAS_EXEC_GET_PAGE_BUILDER) {
            return Exec.getPageBuilder(bufferAllocator, schema, output);
        } else {
            return new PageBuilder(bufferAllocator, schema, output);
        }
    }

    private static boolean hasExecGetPageBuilder() {
        try {
            Exec.class.getMethod("getPageBuilder", BufferAllocator.class, Schema.class, PageOutput.class);
        } catch (final NoSuchMethodException ex) {
            return false;
        }
        return true;
    }

    private static final boolean HAS_EXEC_GET_PAGE_BUILDER = hasExecGetPageBuilder();

    private final Class<T> taskClass;
    private final ConfigDiffBuildable<T> configDiffBuilder;
    private final InputTaskValidatable<T> inputTaskValidator;
    private final ServiceDataSplitterBuildable<T> serviceDataSplitterBuilder;
    private final ServiceDataIngestable<T> serviceDataIngester;
    private final ServiceResponseMapperBuildable<T> serviceResponseMapperBuilder;
}
