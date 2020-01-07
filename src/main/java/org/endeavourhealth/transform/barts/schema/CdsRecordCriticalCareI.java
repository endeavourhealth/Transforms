package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;

import java.text.DateFormat;

public interface CdsRecordCriticalCareI {

    CsvCell getCdsUniqueId();
    CsvCell getLocalPatientId();
    CsvCell getNhsNumber();
    CsvCell getSpellNumber();
    CsvCell getEpisodeNumber();
    CsvCell getCriticalCareTypeID();
    CsvCell getCriticalCareIdentifier();
    CsvCell getCriticalCareStartDate();
    CsvCell getCriticalCareStartTime();
    CsvCell getCriticalCareUnitFunction();
    CsvCell getCriticalCareAdmissionSource();
    CsvCell getCriticalCareSourceLocation();
    CsvCell getCriticalCareAdmissionType();
    CsvCell getGestationLengthAtDelivery();
    CsvCell getAdvancedRespiratorySupportDays();
    CsvCell getBasicRespiratorySupportsDays();
    CsvCell getAdvancedCardiovascularSupportDays();
    CsvCell getBasicCardiovascularSupportDays();
    CsvCell getRenalSupportDays();
    CsvCell getNeurologicalSupportDays();
    CsvCell getGastroIntestinalSupportDays();
    CsvCell getDermatologicalSupportDays();
    CsvCell getLiverSupportDays();
    CsvCell getOrganSupportMaximum();
    CsvCell getCriticalCareLevel2Days();
    CsvCell getCriticalCareLevel3Days();
    CsvCell getCriticalCareDischargeDate();
    CsvCell getCriticalCareDischargeTime();
    CsvCell getCriticalCareDischargeReadyDate();
    CsvCell getCriticalCareDischargeReadyTime();
    CsvCell getCriticalCareDischargeStatus();
    CsvCell getCriticalCareDischargeDestination();
    CsvCell getCriticalCareDischargeLocation();
    CsvCell getCareActivity1();
    CsvCell getCareActivity2100();

    CsvCurrentState getCurrentState();
    DateFormat getDateFormat();
}