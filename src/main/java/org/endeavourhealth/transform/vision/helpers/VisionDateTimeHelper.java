package org.endeavourhealth.transform.vision.helpers;

import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.TemporalPrecisionEnum;

import java.util.Date;

public class VisionDateTimeHelper {

    /**
     * turns the separate DATE and TIME cells into a FHIR DateTimeType with a suitable precision set
     */
    public static DateTimeType getDateTime(CsvCell dateCell, CsvCell timeCell) throws Exception {

        //there is some bad data in the "TIME" column in the test pack
        if (timeCell != null && !timeCell.isEmpty()) {
            String timeStr = timeCell.getString();
            //if the time cell has a bad time, just null the variable so we only use the date
            if (timeStr.equals("9999")) {
                timeCell = null;
            }
        }

        Date dt = CsvCell.getDateTimeFromTwoCells(dateCell, timeCell);
        if (dt == null) {
            return null;
        }

        if (timeCell == null || timeCell.isEmpty()) {
            return new DateTimeType(dt, TemporalPrecisionEnum.DAY);

        } else {
            //although we only have Vision times in hours and minutes, DateTimeType
            //does not support minute precision, so treat it as seconds too
            return new DateTimeType(dt, TemporalPrecisionEnum.SECOND);
            //return new DateTimeType(dt, TemporalPrecisionEnum.MINUTE);
        }
    }
}
