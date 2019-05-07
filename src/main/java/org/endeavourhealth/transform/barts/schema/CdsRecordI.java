package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;

import java.text.DateFormat;

public interface CdsRecordI {

    CsvCell getPrimaryProcedureOPCS();
    CsvCell getWithheldFlag();
    CsvCell getCdsUniqueId();
    CsvCell getCdsActivityDate();
    CsvCell getCdsUpdateType();
    CsvCell getLocalPatientId();
    CsvCell getNhsNumber();
    CsvCell getPersonBirthDate();
    CsvCell getConsultantCode();
    CsvCell getPrimaryProcedureDate();
    CsvCell getSecondaryProcedureOPCS();
    CsvCell getSecondaryProcedureDate();
    CsvCell getAdditionalSecondaryProceduresOPCS();

    CsvCurrentState getCurrentState();
    DateFormat getDateFormat();
}
