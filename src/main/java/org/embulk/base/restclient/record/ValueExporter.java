package org.embulk.base.restclient.record;

public class ValueExporter
{
    public ValueExporter(EmbulkValueScope embulkValueScope, ValueLocator valueLocator)
    {
        this.embulkValueScope = embulkValueScope;
        this.valueLocator = valueLocator;
    }

    public void exportValueToBuildRecord(SinglePageRecordReader singlePageRecordReader,
                                         ServiceRecord.Builder serviceRecordBuilder)
    {
        ServiceValue serviceValue = this.embulkValueScope.scopeEmbulkValues(singlePageRecordReader);
        serviceRecordBuilder.add(serviceValue, this.valueLocator);
    }

    private final EmbulkValueScope embulkValueScope;
    private final ValueLocator valueLocator;
}
