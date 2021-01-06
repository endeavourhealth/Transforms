package org.endeavourhealth.transform.tpp.csv.helpers.cache;

import org.endeavourhealth.transform.common.CsvCell;

import java.util.Date;

public class RotaDetailsObject {

    private Date startDate; //not persisted as a CsvCell because we can't parse into date when not linked to a Parser
    private CsvCell clinicianProfileIdCell;

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public CsvCell getClinicianProfileIdCell() {
        return clinicianProfileIdCell;
    }

    public void setClinicianProfileIdCell(CsvCell clinicianProfileIdCell) {
        this.clinicianProfileIdCell = clinicianProfileIdCell;
    }
}
