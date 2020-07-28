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

package org.embulk.base.restclient.jackson;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JacksonJsonPointerValueLocator extends JacksonValueLocator {
    public JacksonJsonPointerValueLocator(final String pointerString) {
        this.pointer = JsonPointer.compile(pointerString);
    }

    public JacksonJsonPointerValueLocator(final JsonPointer pointer) {
        this.pointer = pointer;
    }

    @Override
    public JsonNode seekValue(final ObjectNode record) {
        return record.at(this.pointer);
    }

    @Override
    public void placeValue(final ObjectNode record, final JsonNode value) {
        final JsonPointer head = this.pointer.head();
        final JsonPointer last = this.pointer.last();
        if (last.mayMatchProperty()) {  // Can be an index of an array, or a property of an object
            final JsonNode parent = record.at(head);
            if (last.mayMatchElement()) {  // Can be an index of an array
                if (parent.isArray()) {
                    ((ArrayNode) parent).set(last.getMatchingIndex(), value);
                } else if (parent.isObject()) {
                    ((ObjectNode) parent).set(last.getMatchingProperty(), value);
                } else {
                    throw new RuntimeException("Placing a property onto non-object nor non-array.");
                }
            } else {  // Must be a property of an object, not an index of an array
                if (parent.isObject()) {
                    ((ObjectNode) parent).set(last.getMatchingProperty(), value);
                } else {
                    throw new RuntimeException("Placing a property onto non-object.");
                }
            }
        } else {
            throw new RuntimeException("FATAL: JSON Pointer must not match any element.");
        }
    }

    private final JsonPointer pointer;
}
