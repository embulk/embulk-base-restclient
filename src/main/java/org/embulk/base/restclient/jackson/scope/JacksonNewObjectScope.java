package org.embulk.base.restclient.jackson.scope;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.base.restclient.record.SinglePageRecordReader;

public class JacksonNewObjectScope extends JacksonObjectScopeBase {
    @Override
    public ObjectNode scopeObject(final SinglePageRecordReader singlePageRecordReader) {
        return JsonNodeFactory.instance.objectNode();
    }
}
