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
