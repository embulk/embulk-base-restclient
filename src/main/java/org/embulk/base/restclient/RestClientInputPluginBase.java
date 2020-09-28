/*
 * Copyright 2016 The Embulk project
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
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.util.config.ConfigMapperFactory;

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
            final ConfigMapperFactory configMapperFactory,
            final Class<T> taskClass,
            final ConfigDiffBuildable<T> configDiffBuilder,
            final InputTaskValidatable<T> inputTaskValidator,
            final ServiceDataIngestable<T> serviceDataIngester,
            final ServiceDataSplitterBuildable<T> serviceDataSplitterBuilder,
            final ServiceResponseMapperBuildable<T> serviceResponseMapperBuilder) {
        super(configMapperFactory,
              taskClass,
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
            final ConfigMapperFactory configMapperFactory,
            final Class<T> taskClass,
            final ConfigDiffBuildable<T> configDiffBuilder,
            final InputTaskValidatable<T> inputTaskValidator,
            final ServiceDataIngestable<T> serviceDataIngester,
            final ServiceResponseMapperBuildable<T> serviceResponseMapperBuilder) {
        super(configMapperFactory,
              taskClass,
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
    protected RestClientInputPluginBase(
            final ConfigMapperFactory configMapperFactory,
            final Class<T> taskClass,
            final RestClientInputPluginDelegate<T> delegate) {
        super(configMapperFactory, taskClass, delegate, delegate, delegate, delegate, delegate);
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
