package org.embulk.base.restclient.record;

import java.util.List;

public class RecordExporter
{
    public RecordExporter(List<ValueExporter> valueExporters, ServiceRecord.Builder serviceRecordBuilder)
    {
        this.valueExporters = valueExporters;
        this.serviceRecordBuilder = serviceRecordBuilder;
    }

    public ServiceRecord exportRecord(SinglePageRecordReader singlePageRecordReader)
    {
        this.serviceRecordBuilder.reset();
        for (ValueExporter valueExporter : this.valueExporters) {
            // |EmbulkValueScope| and |ValueLocator| are contained in |ValueExporter|.
            valueExporter.exportValueToBuildRecord(singlePageRecordReader, this.serviceRecordBuilder);
        }
        return this.serviceRecordBuilder.build();
    }

    private final List<ValueExporter> valueExporters;
    private final ServiceRecord.Builder serviceRecordBuilder;
}
