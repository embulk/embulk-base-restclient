package org.embulk.base.restclient.record;

public class ValueExporter {
    public ValueExporter(final EmbulkValueScope embulkValueScope, final ValueLocator valueLocator) {
        this.embulkValueScope = embulkValueScope;
        this.valueLocator = valueLocator;
    }

    public void exportValueToBuildRecord(
            final SinglePageRecordReader singlePageRecordReader, final ServiceRecord.Builder serviceRecordBuilder) {
        final ServiceValue serviceValue = this.embulkValueScope.scopeEmbulkValues(singlePageRecordReader);
        serviceRecordBuilder.add(serviceValue, this.valueLocator);
    }

    private final EmbulkValueScope embulkValueScope;
    private final ValueLocator valueLocator;
}
