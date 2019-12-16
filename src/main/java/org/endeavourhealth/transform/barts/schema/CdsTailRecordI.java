package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;

public interface CdsTailRecordI {

    CsvCell getPersonId();
    CsvCell getCdsUniqueId();
    CsvCell getCdsUpdateType();
    CsvCell getLocalPatientId();
    CsvCell getNhsNumber();
    CsvCell getEncounterId();
    CsvCell getEpisodeId();
    CsvCell getResponsiblePersonnelId();
    CsvCell getTreatmentFunctionCd();

    CsvCurrentState getCurrentState();
}
