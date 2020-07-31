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

import com.google.common.base.Preconditions;
import java.util.List;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

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
            final Class<T> taskClass,
            final ConfigDiffBuildable<T> configDiffBuilder,
            final InputTaskValidatable<T> inputTaskValidator,
            final ServiceDataIngestable<T> serviceDataIngester,
            final ServiceDataSplitterBuildable<T> serviceDataSplitterBuilder,
            final ServiceResponseMapperBuildable<T> serviceResponseMapperBuilder) {
        this.taskClass = taskClass;
        this.configDiffBuilder = configDiffBuilder;
        this.inputTaskValidator = inputTaskValidator;
        this.serviceDataIngester = serviceDataIngester;
        this.serviceDataSplitterBuilder = serviceDataSplitterBuilder;
        this.serviceResponseMapperBuilder = serviceResponseMapperBuilder;
    }

    protected RestClientInputPluginBaseUnsafe(
            final Class<T> taskClass,
            final ConfigDiffBuildable<T> configDiffBuilder,
            final InputTaskValidatable<T> inputTaskValidator,
            final ServiceDataIngestable<T> serviceDataIngester,
            final ServiceResponseMapperBuildable<T> serviceResponseMapperBuilder) {
        this(
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

    protected RestClientInputPluginBaseUnsafe(final Class<T> taskClass, final RestClientInputPluginDelegate<T> delegate) {
        this(taskClass, delegate, delegate, delegate, delegate, delegate);
    }

    @Override
    public ConfigDiff transaction(final ConfigSource config, final InputPlugin.Control control) {
        final T task = loadConfig(config, this.taskClass);
        this.inputTaskValidator.validateInputTask(task);

        final int taskCount = this.serviceDataSplitterBuilder
                .buildServiceDataSplitter(task)
                .numberToSplitWithHintingInTask(task);

        final Schema schema = this.serviceResponseMapperBuilder.buildServiceResponseMapper(task).getEmbulkSchema();
        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(
            final TaskSource taskSource, final Schema schema, final int taskCount, final InputPlugin.Control control) {
        final T task = taskSource.loadTask(this.taskClass);
        final List<TaskReport> taskReports = control.run(taskSource, schema, taskCount);
        return this.configDiffBuilder.buildConfigDiff(task, schema, taskCount, taskReports);
    }

    @Override
    public void cleanup(
            final TaskSource taskSource, final Schema schema, final int taskCount, final List<TaskReport> successTaskReports) {
    }

    @Override
    public TaskReport run(final TaskSource taskSource, final Schema schema, final int taskIndex, final PageOutput output) {
        final T task = taskSource.loadTask(this.taskClass);
        this.serviceDataSplitterBuilder
            .buildServiceDataSplitter(task)
            .hintInEachSplitTask(task, schema, taskIndex);

        final ServiceResponseMapper<? extends ValueLocator> serviceResponseMapper =
                this.serviceResponseMapperBuilder.buildServiceResponseMapper(task);

        try (final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
            // When failing around |PageBuidler| in |ingestServiceData|, |pageBuilder.finish()| should not be called.
            final TaskReport taskReport = Preconditions.checkNotNull(this.serviceDataIngester.ingestServiceData(
                    task,
                    serviceResponseMapper.createRecordImporter(),
                    taskIndex,
                    pageBuilder));
            pageBuilder.finish();
            return taskReport;
        }
    }

    @Override
    public ConfigDiff guess(final ConfigSource config) {
        return Exec.newConfigDiff();
    }

    private final Class<T> taskClass;
    private final ConfigDiffBuildable<T> configDiffBuilder;
    private final InputTaskValidatable<T> inputTaskValidator;
    private final ServiceDataSplitterBuildable<T> serviceDataSplitterBuilder;
    private final ServiceDataIngestable<T> serviceDataIngester;
    private final ServiceResponseMapperBuildable<T> serviceResponseMapperBuilder;
}
