package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.CsvCell;

public interface CdsRecordOutpatientI extends CdsRecordI {

    CsvCell getAttendanceIdentifier();
    CsvCell getAdministrativeCategoryCode();
    CsvCell getAppointmentAttendedCode();
    CsvCell getAppointmentOutcomeCode();
    CsvCell getAppointmentDate();
    CsvCell getAppointmentTime();
    CsvCell getAppointmentSiteCode();
}
