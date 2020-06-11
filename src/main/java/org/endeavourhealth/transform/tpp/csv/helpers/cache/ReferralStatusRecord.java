package org.endeavourhealth.transform.tpp.csv.helpers.cache;

import org.endeavourhealth.transform.common.CsvCell;

public class ReferralStatusRecord {

    private CsvCell dateCell;
    private CsvCell referralStatusCell;

    public ReferralStatusRecord(CsvCell dateCell, CsvCell referralStatusCell) {
        this.dateCell = dateCell;
        this.referralStatusCell = referralStatusCell;
    }

    public CsvCell getDateCell() {
        return dateCell;
    }

    public CsvCell getReferralStatusCell() {
        return referralStatusCell;
    }
}
