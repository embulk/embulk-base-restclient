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

package org.embulk.base.restclient.jackson.scope;

import org.embulk.base.restclient.record.SinglePageRecordReader;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;

public class JacksonDirectStringScope extends JacksonStringScopeBase {
    public JacksonDirectStringScope(final String embulkColumnName) {
        this.embulkColumnName = embulkColumnName;
    }

    @Override
    public String scopeString(final SinglePageRecordReader singlePageRecordReader) {
        final Schema embulkSchema = singlePageRecordReader.getSchema();
        final Column column = cacheSingleColumn(embulkSchema, this.embulkColumnName);
        return singlePageRecordReader.getString(column);
    }

    private final String embulkColumnName;
}
