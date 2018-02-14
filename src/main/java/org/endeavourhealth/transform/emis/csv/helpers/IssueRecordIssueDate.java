package org.endeavourhealth.transform.emis.csv.helpers;

import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.DateType;

import java.util.Date;

public class IssueRecordIssueDate {

    private DateType issueDateType = null;
    private CsvCell[] sourceCells = null;

    public IssueRecordIssueDate(DateTimeType issueDateTime, CsvCell... sourceCells) {
        this.issueDateType = new DateType(issueDateTime.getValue());
        this.sourceCells = sourceCells;
    }

    public DateType getIssueDateType() {
        return issueDateType;
    }

    public CsvCell[] getSourceCells() {
        return sourceCells;
    }

    public boolean afterOrOtherIsNull(DateType otherDateType) {
        if (otherDateType == null) {
            return true;
        } else {
            Date ourDate = issueDateType.getValue();
            Date otherDate = otherDateType.getValue();
            return ourDate.after(otherDate);
        }
    }

    public boolean beforeOrOtherIsNull(DateType otherDateType) {
        if (otherDateType == null) {
            return true;
        } else {
            Date ourDate = issueDateType.getValue();
            Date otherDate = otherDateType.getValue();
            return ourDate.before(otherDate);
        }
    }

    public boolean afterOrOtherIsNull(IssueRecordIssueDate other) {
        if (other == null) {
            return true;
        } else {
            DateType otherDateType = other.issueDateType;
            return afterOrOtherIsNull(otherDateType);
        }
    }

    public boolean beforeOrOtherIsNull(IssueRecordIssueDate other) {
        if (other == null) {
            return true;
        } else {
            DateType otherDateType = other.issueDateType;
            return beforeOrOtherIsNull(otherDateType);
        }
    }
}
