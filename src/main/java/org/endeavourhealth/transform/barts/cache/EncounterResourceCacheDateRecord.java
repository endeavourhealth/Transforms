package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.transform.common.CsvCell;

import java.util.Date;

public class EncounterResourceCacheDateRecord {
    private String encounterId;
    private Date beginDate;
    private Date endDate;
    private CsvCell beginDateCell;
    private CsvCell endDateCell;

    public String getEncounterId() {
        return encounterId;
    }

    public void setEncounterId(String encounterId) {
        this.encounterId = encounterId;
    }

    public Date getBeginDate() {
        return beginDate;
    }

    public void setBeginDate(Date beginDate) {
        this.beginDate = beginDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public CsvCell getBeginDateCell() {
        return beginDateCell;
    }

    public void setBeginDateCell(CsvCell beginDateCell) {
        this.beginDateCell = beginDateCell;
    }

    public CsvCell getEndDateCell() {
        return endDateCell;
    }

    public void setEndDateCell(CsvCell endDateCell) {
        this.endDateCell = endDateCell;
    }
}
