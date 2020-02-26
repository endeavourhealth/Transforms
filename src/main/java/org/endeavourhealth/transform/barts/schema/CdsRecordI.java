package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;

import java.text.DateFormat;

public interface CdsRecordI {

    CsvCell getCDSRecordType();
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
    CsvCell getPrimaryDiagnosisICD();
    CsvCell getSecondaryDiagnosisICD();
    CsvCell getAdditionalSecondaryDiagnosisICD();
    CsvCell getPatientPathwayIdentifier();

    CsvCurrentState getCurrentState();
    DateFormat getDateFormat();
}
