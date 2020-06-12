package org.endeavourhealth.transform.tpp.csv.helpers.cache;

import org.endeavourhealth.transform.common.CsvCell;

public class MedicalRecordStatusCacheObject {
    private CsvCell dateCell;
    private CsvCell statusCell;

    public MedicalRecordStatusCacheObject(CsvCell dateCell, CsvCell statusCell) {
        this.dateCell = dateCell;
        this.statusCell = statusCell;
    }

    public CsvCell getDateCell() {
        return dateCell;
    }

    public CsvCell getStatusCell() {
        return statusCell;
    }
}
