package org.endeavourhealth.transform.emis.csv.helpers;

import org.endeavourhealth.transform.common.CsvCell;

public class BpComponent {
    private CsvCell codeId;
    private CsvCell value;
    private CsvCell unit;

    public BpComponent(CsvCell codeId, CsvCell value, CsvCell unit) {
        this.codeId = codeId;
        this.value = value;
        this.unit = unit;
    }

    public CsvCell getCodeId() {
        return codeId;
    }

    public CsvCell getValue() {
        return value;
    }

    public CsvCell getUnit() {
        return unit;
    }
}
