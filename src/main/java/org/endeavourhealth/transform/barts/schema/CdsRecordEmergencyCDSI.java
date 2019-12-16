package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;

import java.text.DateFormat;

public interface CdsRecordEmergencyCDSI {

    CsvCell getWithheldReason();  //no flag, only a reason
    CsvCell getCdsUniqueId();
    CsvCell getCdsActivityDate();
    CsvCell getCdsUpdateType();
    CsvCell getLocalPatientId();
    CsvCell getNhsNumber();
    CsvCell getPersonBirthDate();

    CsvCell getPatientPathwayIdentifier();
    CsvCell getDepartmentType();
    CsvCell getAmbulanceIncidentNumber();
    CsvCell getAmbulanceTrustOrganisationCode();
    CsvCell getAttendanceIdentifier();
    CsvCell getArrivalMode();
    CsvCell getAttendanceCategory();
    CsvCell getAttendanceSource();
    CsvCell getArrivalDate();
    CsvCell getArrivalTime();
    CsvCell getInitialAssessmentDate();
    CsvCell getInitialAssessmentTime();
    CsvCell getChiefComplaint();
    CsvCell getDateSeenforTreatment();
    CsvCell getTimeSeenforTreatment();
    CsvCell getDecidedtoAdmitDate();
    CsvCell getDecidedtoAdmitTime();
    CsvCell getActivityTreatmentFunctionCode();
    CsvCell getDischargeStatus();
    CsvCell getConclusionDate();
    CsvCell getConclusionTime();
    CsvCell getDepartureDate();
    CsvCell getDepartureTime();
    CsvCell getDischargeDestination();

    //Mental Health Classifications 1 - 10
    CsvCell getMHClassificationCode(int dataNumber);
    CsvCell getMHClassificationStartDate(int dataNumber);
    CsvCell getMHClassificationStartTime(int dataNumber);
    CsvCell getMHClassificationEndDate(int dataNumber);
    CsvCell getMHClassificationEndTime(int dataNumber);

    //Diagnosis 1 - 20
    CsvCell getDiagnosis(int dataNumber);

    //Investigations 1 - 20
    CsvCell getInvestigation(int dataNumber);
    CsvCell getInvestigationPerformedDate(int dataNumber);
    CsvCell getInvestigationPerformedTime(int dataNumber);

    //Treatments 1 - 20
    CsvCell getTreatment(int dataNumber);
    CsvCell getTreatmentDate(int dataNumber);
    CsvCell getTreatmentTime(int dataNumber);

    //Referrals 1 - 10
    CsvCell getReferralToService(int dataNumber);
    CsvCell getReferralRequestDate(int dataNumber);
    CsvCell getReferralRequestTime(int dataNumber);
    CsvCell getReferralAssessmentDate(int dataNumber);
    CsvCell getReferralAssessmentTime(int dataNumber);

    //Safe Guarding 1 - 10
    CsvCell getSafeguardingConcern(int dataNumber);

    CsvCurrentState getCurrentState();
    DateFormat getDateFormat();
}