package org.endeavourhealth.transform.emis.csv.helpers;

import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.DateTimeType;

public class CodeAndDate {

    private EmisCsvCodeMap codeMapping;
    private DateTimeType date;
    private CsvCell[] additionalSourceCells;

    public CodeAndDate(EmisCsvCodeMap codeMapping, DateTimeType date, CsvCell... additionalSourceCells) {
        this.codeMapping = codeMapping;
        this.date = date;
        this.additionalSourceCells = additionalSourceCells;
    }

    public EmisCsvCodeMap getCodeMapping() {
        return codeMapping;
    }

    public DateTimeType getDate() {
        return date;
    }

    public CsvCell[] getAdditionalSourceCells() {
        return additionalSourceCells;
    }

    public boolean isAfterOrOtherIsNull(CodeAndDate other) {
        if (other == null) {
            return true;
        }

        DateTimeType ourDate = getDate();
        DateTimeType otherDate = other.getDate();

        //handle having a null date
        if (ourDate == null && otherDate == null) {
            //if both dates are null OUR data is not AFTER the other date
            return false;

        } else if (ourDate == null) {
            //if our date is null, the other date must be more resnet
            return false;

        } else if (otherDate == null) {
            //if the other date is null, our date must be more recent
            return true;

        } else {
            return ourDate.after(otherDate);
        }
    }

    /*public boolean isBefore(DateTimeType other) {
        if (date == null) {
            return true;
        } else if (other == null) {
            return false;
        } else {
            return date.before(other);
        }

    }*/
}
