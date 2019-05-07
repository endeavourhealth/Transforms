package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;

public interface CdsTailIRecordI {

    CsvCell getPersonId();
    CsvCell getCdsUniqueId();
    CsvCell getCdsUpdateType();
    CsvCell getLocalPatientId();
    CsvCell getNhsNumber();
    CsvCell getEncounterId();
    CsvCell getResponsiblePersonnelId();

    CsvCurrentState getCurrentState();
}
