package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import static org.endeavourhealth.transform.barts.schema.EmergencyCareDataSetStatics.V1_DATA_SET;

public class SusEmergencyCareDataSet extends AbstractCsvParser implements CdsRecordEmergencyCDSI {
    private static final Logger LOG = LoggerFactory.getLogger(SusEmergencyCareDataSet.class);

    public SusEmergencyCareDataSet(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        //confusingly the ECDS file is a pipe-delimited file, but uses the CDS date and time formats,
        //plus the Tails file is fixed width rather than delimited
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT.withHeader(getCsvHeadersForVersion(version)), //no headers in the file
                BartsCsvToFhirTransformer.CDS_DATE_FORMAT,
                BartsCsvToFhirTransformer.CDS_TIME_FORMAT);
    }

    private static String[] getCsvHeadersForVersion(String version) {
        return V1_DATA_SET;
    }

    public CsvCell getCdsUniqueId() {
        return super.getCell("CDSUniqueIdentifier");
    }
    public CsvCell getLocalPatientId() { return super.getCell("LocalPatientID");}
    public CsvCell getNhsNumber() { return super.getCell("NHSNumber");}
    public CsvCell getCdsActivityDate() {return super.getCell("CDSActivityDate");}
    public CsvCell getCdsUpdateType() { return super.getCell("CDSUpdateType");}
    public CsvCell getPersonBirthDate() { return super.getCell("DateofBirth");}
    public CsvCell getWithheldReason() { return super.getCell("WithheldReason");}

    public CsvCell getPatientPathwayIdentifier() { return super.getCell("PatientPathwayIdentifier");}
    public CsvCell getDepartmentType() { return super.getCell("EmergencyCareDepartmentType");}
    public CsvCell getAmbulanceIncidentNumber() { return super.getCell("AmbulanceIncidentNumber");}
    public CsvCell getTreatmentOrganisationCode() { return super.getCell("OrganisationSiteIdentifierofTreatment");}
    public CsvCell getAttendanceIdentifier() { return super.getCell("EmergencyCareAttendanceIdentifier");}
    public CsvCell getArrivalMode() { return super.getCell("EmergencyCareArrivalMode");}
    public CsvCell getAttendanceCategory() { return super.getCell("EmergencyCareAttendanceCategory");}
    public CsvCell getAttendanceSource() { return super.getCell("EmergencyCareAttendanceSource");}
    public CsvCell getArrivalDate() { return super.getCell("EmergencyCareArrivalDate");}
    public CsvCell getArrivalTime() { return super.getCell("EmergencyCareArrivalTime");}
    public CsvCell getInitialAssessmentDate() { return super.getCell("EmergencyCareInitialAssessmentDate");}
    public CsvCell getInitialAssessmentTime() { return super.getCell("EmergencyCareInitialAssessmentTime");}
    public CsvCell getChiefComplaint() { return super.getCell("EmergencyCareChiefComplaint");}
    public CsvCell getDateSeenforTreatment() { return super.getCell("EmergencyCareDateSeenforTreatment");}
    public CsvCell getTimeSeenforTreatment() { return super.getCell("EmergencyCareTimeSeenforTreatment");}
    public CsvCell getDecidedtoAdmitDate() { return super.getCell("DecidedtoAdmitDate");}
    public CsvCell getDecidedtoAdmitTime() { return super.getCell("DecidedtoAdmitTime");}
    public CsvCell getActivityTreatmentFunctionCode() { return super.getCell("ActivityTreatmentFunctionCode");}
    public CsvCell getDischargeStatus() { return super.getCell("EmergencyCareDischargeStatusSnomed");}
    public CsvCell getConclusionDate() { return super.getCell("EmergencyCareConclusionDate");}
    public CsvCell getConclusionTime() { return super.getCell("EmergencyCareConclusionTime");}
    public CsvCell getDepartureDate() { return super.getCell("EmergencyCareDepartureDate");}
    public CsvCell getDepartureTime() { return super.getCell("EmergencyCareDepartureTime");}
    public CsvCell getDischargeDestination() { return super.getCell("EmergencyCareDischargeDestination");}
    public CsvCell getDischargeDestinationSiteId() { return super.getCell("OrganisationSiteIdentifierDischargeFromEmergencyCare");}
    public CsvCell getDischargeFollowUp() { return super.getCell("EmergencyCareDischargeFollowup");}

    //mental health classifications 1-10
    public CsvCell getMHClassificationCode(int dataNumber) { return super.getCell("MHALegalStatusClassificationCode"+dataNumber);}
    public CsvCell getMHClassificationStartDate(int dataNumber) { return super.getCell("StartDateMHALegalStatusClassificationAssignmentPeriod"+dataNumber);}
    public CsvCell getMHClassificationStartTime(int dataNumber) { return super.getCell("StartTimeMHALegalStatusClassificationAssignmentPeriod"+dataNumber);}
    public CsvCell getMHClassificationEndDate(int dataNumber) { return super.getCell("ExpiryDateMHALegalStatusClassification"+dataNumber);}
    public CsvCell getMHClassificationEndTime(int dataNumber) { return super.getCell("ExpiryTimeMHALegalStatusClassification"+dataNumber);}

    public CsvCell getMHClassificationStartDate1() { return super.getCell("StartDateMHALegalStatusClassificationAssignmentPeriod1");}
    public CsvCell getMHClassificationStartTime1() { return super.getCell("StartTimeMHALegalStatusClassificationAssignmentPeriod1");}
    public CsvCell getMHClassificationEndDate1() { return super.getCell("ExpiryDateMHALegalStatusClassification1");}
    public CsvCell getMHClassificationEndTime1() { return super.getCell("ExpiryTimeMHALegalStatusClassification1");}
    public CsvCell getMHClassificationCode1() { return super.getCell("MHALegalStatusClassificationCode1");}
    public CsvCell getMHClassificationStartDate2() { return super.getCell("StartDateMHALegalStatusClassificationAssignmentPeriod2");}
    public CsvCell getMHClassificationStartTime2() { return super.getCell("StartTimeMHALegalStatusClassificationAssignmentPeriod2");}
    public CsvCell getMHClassificationEndDate2() { return super.getCell("ExpiryDateMHALegalStatusClassification2");}
    public CsvCell getMHClassificationEndTime2() { return super.getCell("ExpiryTimeMHALegalStatusClassification2");}
    public CsvCell getMHClassificationCode2() { return super.getCell("MHALegalStatusClassificationCode2");}
    public CsvCell getMHClassificationStartDate3() { return super.getCell("StartDateMHALegalStatusClassificationAssignmentPeriod3");}
    public CsvCell getMHClassificationStartTime3() { return super.getCell("StartTimeMHALegalStatusClassificationAssignmentPeriod3");}
    public CsvCell getMHClassificationEndDate3() { return super.getCell("ExpiryDateMHALegalStatusClassification3");}
    public CsvCell getMHClassificationEndTime3() { return super.getCell("ExpiryTimeMHALegalStatusClassification3");}
    public CsvCell getMHClassificationCode3() { return super.getCell("MHALegalStatusClassificationCode3");}
    public CsvCell getMHClassificationStartDate4() { return super.getCell("StartDateMHALegalStatusClassificationAssignmentPeriod4");}
    public CsvCell getMHClassificationStartTime4() { return super.getCell("StartTimeMHALegalStatusClassificationAssignmentPeriod4");}
    public CsvCell getMHClassificationEndDate4() { return super.getCell("ExpiryDateMHALegalStatusClassification4");}
    public CsvCell getMHClassificationEndTime4() { return super.getCell("ExpiryTimeMHALegalStatusClassification4");}
    public CsvCell getMHClassificationCode4() { return super.getCell("MHALegalStatusClassificationCode4");}
    public CsvCell getMHClassificationStartDate5() { return super.getCell("StartDateMHALegalStatusClassificationAssignmentPeriod5");}
    public CsvCell getMHClassificationStartTime5() { return super.getCell("StartTimeMHALegalStatusClassificationAssignmentPeriod5");}
    public CsvCell getMHClassificationEndDate5() { return super.getCell("ExpiryDateMHALegalStatusClassification5");}
    public CsvCell getMHClassificationEndTime5() { return super.getCell("ExpiryTimeMHALegalStatusClassification5");}
    public CsvCell getMHClassificationCode5() { return super.getCell("MHALegalStatusClassificationCode5");}
    public CsvCell getMHClassificationStartDate6() { return super.getCell("StartDateMHALegalStatusClassificationAssignmentPeriod6");}
    public CsvCell getMHClassificationStartTime6() { return super.getCell("StartTimeMHALegalStatusClassificationAssignmentPeriod6");}
    public CsvCell getMHClassificationEndDate6() { return super.getCell("ExpiryDateMHALegalStatusClassification6");}
    public CsvCell getMHClassificationEndTime6() { return super.getCell("ExpiryTimeMHALegalStatusClassification6");}
    public CsvCell getMHClassificationCode6() { return super.getCell("MHALegalStatusClassificationCode6");}
    public CsvCell getMHClassificationStartDate7() { return super.getCell("StartDateMHALegalStatusClassificationAssignmentPeriod7");}
    public CsvCell getMHClassificationStartTime7() { return super.getCell("StartTimeMHALegalStatusClassificationAssignmentPeriod7");}
    public CsvCell getMHClassificationEndDate7() { return super.getCell("ExpiryDateMHALegalStatusClassification7");}
    public CsvCell getMHClassificationEndTime7() { return super.getCell("ExpiryTimeMHALegalStatusClassification7");}
    public CsvCell getMHClassificationCode7() { return super.getCell("MHALegalStatusClassificationCode7");}
    public CsvCell getMHClassificationStartDate8() { return super.getCell("StartDateMHALegalStatusClassificationAssignmentPeriod8");}
    public CsvCell getMHClassificationStartTime8() { return super.getCell("StartTimeMHALegalStatusClassificationAssignmentPeriod8");}
    public CsvCell getMHClassificationEndDate8() { return super.getCell("ExpiryDateMHALegalStatusClassification8");}
    public CsvCell getMHClassificationEndTime8() { return super.getCell("ExpiryTimeMHALegalStatusClassification8");}
    public CsvCell getMHClassificationCode8() { return super.getCell("MHALegalStatusClassificationCode8");}
    public CsvCell getMHClassificationStartDate9() { return super.getCell("StartDateMHALegalStatusClassificationAssignmentPeriod9");}
    public CsvCell getMHClassificationStartTime9() { return super.getCell("StartTimeMHALegalStatusClassificationAssignmentPeriod9");}
    public CsvCell getMHClassificationEndDate9() { return super.getCell("ExpiryDateMHALegalStatusClassification9");}
    public CsvCell getMHClassificationEndTime9() { return super.getCell("ExpiryTimeMHALegalStatusClassification9");}
    public CsvCell getMHClassificationCode9() { return super.getCell("MHALegalStatusClassificationCode9");}
    public CsvCell getMHClassificationStartDate10() { return super.getCell("StartDateMHALegalStatusClassificationAssignmentPeriod10");}
    public CsvCell getMHClassificationStartTime10() { return super.getCell("StartTimeMHALegalStatusClassificationAssignmentPeriod10");}
    public CsvCell getMHClassificationEndDate10() { return super.getCell("ExpiryDateMHALegalStatusClassification10");}
    public CsvCell getMHClassificationEndTime10() { return super.getCell("ExpiryTimeMHALegalStatusClassification10");}
    public CsvCell getMHClassificationCode10() { return super.getCell("MHALegalStatusClassificationCode10");}

    //Diagnosis 1 - 20
    public CsvCell getDiagnosis(int dataNumber) { return super.getCell("EmergencyCareDiagnosis"+dataNumber);}
    public CsvCell getDiagnosisQualifier(int dataNumber) { return super.getCell("EmergencyCareDiagnosisQualifier"+dataNumber);}

    public CsvCell getDiagnosis1() { return super.getCell("EmergencyCareDiagnosis1");}
    public CsvCell getDiagnosis2() { return super.getCell("EmergencyCareDiagnosis2");}
    public CsvCell getDiagnosis3() { return super.getCell("EmergencyCareDiagnosis3");}
    public CsvCell getDiagnosis4() { return super.getCell("EmergencyCareDiagnosis4");}
    public CsvCell getDiagnosis5() { return super.getCell("EmergencyCareDiagnosis5");}
    public CsvCell getDiagnosis6() { return super.getCell("EmergencyCareDiagnosis6");}
    public CsvCell getDiagnosis7() { return super.getCell("EmergencyCareDiagnosis7");}
    public CsvCell getDiagnosis8() { return super.getCell("EmergencyCareDiagnosis8");}
    public CsvCell getDiagnosis9() { return super.getCell("EmergencyCareDiagnosis9");}
    public CsvCell getDiagnosis10() { return super.getCell("EmergencyCareDiagnosis10");}
    public CsvCell getDiagnosis11() { return super.getCell("EmergencyCareDiagnosis11");}
    public CsvCell getDiagnosis12() { return super.getCell("EmergencyCareDiagnosis12");}
    public CsvCell getDiagnosis13() { return super.getCell("EmergencyCareDiagnosis13");}
    public CsvCell getDiagnosis14() { return super.getCell("EmergencyCareDiagnosis14");}
    public CsvCell getDiagnosis15() { return super.getCell("EmergencyCareDiagnosis15");}
    public CsvCell getDiagnosis16() { return super.getCell("EmergencyCareDiagnosis16");}
    public CsvCell getDiagnosis17() { return super.getCell("EmergencyCareDiagnosis17");}
    public CsvCell getDiagnosis18() { return super.getCell("EmergencyCareDiagnosis18");}
    public CsvCell getDiagnosis19() { return super.getCell("EmergencyCareDiagnosis19");}
    public CsvCell getDiagnosis20() { return super.getCell("EmergencyCareDiagnosis20");}

    //Investigations 1 - 20 (note use of ProcedureDate etc. for date column)
    public CsvCell getInvestigation(int dataNumber) { return super.getCell("EmergencyCareClinicalInvestigation"+dataNumber);}
    public CsvCell getInvestigationPerformedDate(int dataNumber) { return super.getCell("EmergencyCareProcedureDate"+dataNumber);}
    public CsvCell getInvestigationPerformedTime(int dataNumber) { return super.getCell("EmergencyCareProcedureTime"+dataNumber);}

    public CsvCell getInvestigation1() { return super.getCell("EmergencyCareClinicalInvestigation1");}
    public CsvCell getInvestigationPerformedDate1() { return super.getCell("EmergencyCareProcedureDate1");}
    public CsvCell getInvestigationPerformedTime1() { return super.getCell("EmergencyCareProcedureTime1");}
    public CsvCell getInvestigation2() { return super.getCell("EmergencyCareClinicalInvestigation2");}
    public CsvCell getInvestigationPerformedDate2() { return super.getCell("EmergencyCareProcedureDate2");}
    public CsvCell getInvestigationPerformedTime2() { return super.getCell("EmergencyCareProcedureTime2");}
    public CsvCell getInvestigation3() { return super.getCell("EmergencyCareClinicalInvestigation3");}
    public CsvCell getInvestigationPerformedDate3() { return super.getCell("EmergencyCareProcedureDate3");}
    public CsvCell getInvestigationPerformedTime3() { return super.getCell("EmergencyCareProcedureTime3");}
    public CsvCell getInvestigation4() { return super.getCell("EmergencyCareClinicalInvestigation4");}
    public CsvCell getInvestigationPerformedDate4() { return super.getCell("EmergencyCareProcedureDate4");}
    public CsvCell getInvestigationPerformedTime4() { return super.getCell("EmergencyCareProcedureTime4");}
    public CsvCell getInvestigation5() { return super.getCell("EmergencyCareClinicalInvestigation5");}
    public CsvCell getInvestigationPerformedDate5() { return super.getCell("EmergencyCareProcedureDate5");}
    public CsvCell getInvestigationPerformedTime5() { return super.getCell("EmergencyCareProcedureTime5");}
    public CsvCell getInvestigation6() { return super.getCell("EmergencyCareClinicalInvestigation6");}
    public CsvCell getInvestigationPerformedDate6() { return super.getCell("EmergencyCareProcedureDate6");}
    public CsvCell getInvestigationPerformedTime6() { return super.getCell("EmergencyCareProcedureTime6");}
    public CsvCell getInvestigation7() { return super.getCell("EmergencyCareClinicalInvestigation7");}
    public CsvCell getInvestigationPerformedDate7() { return super.getCell("EmergencyCareProcedureDate7");}
    public CsvCell getInvestigationPerformedTime7() { return super.getCell("EmergencyCareProcedureTime7");}
    public CsvCell getInvestigation8() { return super.getCell("EmergencyCareClinicalInvestigation8");}
    public CsvCell getInvestigationPerformedDate8() { return super.getCell("EmergencyCareProcedureDate8");}
    public CsvCell getInvestigationPerformedTime8() { return super.getCell("EmergencyCareProcedureTime8");}
    public CsvCell getInvestigation9() { return super.getCell("EmergencyCareClinicalInvestigation9");}
    public CsvCell getInvestigationPerformedDate9() { return super.getCell("EmergencyCareProcedureDate9");}
    public CsvCell getInvestigationPerformedTime9() { return super.getCell("EmergencyCareProcedureTime9");}
    public CsvCell getInvestigation10() { return super.getCell("EmergencyCareClinicalInvestigation10");}
    public CsvCell getInvestigationPerformedDate10() { return super.getCell("EmergencyCareProcedureDate10");}
    public CsvCell getInvestigationPerformedTime10() { return super.getCell("EmergencyCareProcedureTime10");}
    public CsvCell getInvestigation11() { return super.getCell("EmergencyCareClinicalInvestigation11");}
    public CsvCell getInvestigationPerformedDate11() { return super.getCell("EmergencyCareProcedureDate11");}
    public CsvCell getInvestigationPerformedTime11() { return super.getCell("EmergencyCareProcedureTime11");}
    public CsvCell getInvestigation12() { return super.getCell("EmergencyCareClinicalInvestigation12");}
    public CsvCell getInvestigationPerformedDate12() { return super.getCell("EmergencyCareProcedureDate12");}
    public CsvCell getInvestigationPerformedTime12() { return super.getCell("EmergencyCareProcedureTime12");}
    public CsvCell getInvestigation13() { return super.getCell("EmergencyCareClinicalInvestigation13");}
    public CsvCell getInvestigationPerformedDate13() { return super.getCell("EmergencyCareProcedureDate13");}
    public CsvCell getInvestigationPerformedTime13() { return super.getCell("EmergencyCareProcedureTime13");}
    public CsvCell getInvestigation14() { return super.getCell("EmergencyCareClinicalInvestigation14");}
    public CsvCell getInvestigationPerformedDate14() { return super.getCell("EmergencyCareProcedureDate14");}
    public CsvCell getInvestigationPerformedTime14() { return super.getCell("EmergencyCareProcedureTime14");}
    public CsvCell getInvestigation15() { return super.getCell("EmergencyCareClinicalInvestigation15");}
    public CsvCell getInvestigationPerformedDate15() { return super.getCell("EmergencyCareProcedureDate15");}
    public CsvCell getInvestigationPerformedTime15() { return super.getCell("EmergencyCareProcedureTime15");}
    public CsvCell getInvestigation16() { return super.getCell("EmergencyCareClinicalInvestigation16");}
    public CsvCell getInvestigationPerformedDate16() { return super.getCell("EmergencyCareProcedureDate16");}
    public CsvCell getInvestigationPerformedTime16() { return super.getCell("EmergencyCareProcedureTime16");}
    public CsvCell getInvestigation17() { return super.getCell("EmergencyCareClinicalInvestigation17");}
    public CsvCell getInvestigationPerformedDate17() { return super.getCell("EmergencyCareProcedureDate17");}
    public CsvCell getInvestigationPerformedTime17() { return super.getCell("EmergencyCareProcedureTime17");}
    public CsvCell getInvestigation18() { return super.getCell("EmergencyCareClinicalInvestigation18");}
    public CsvCell getInvestigationPerformedDate18() { return super.getCell("EmergencyCareProcedureDate18");}
    public CsvCell getInvestigationPerformedTime18() { return super.getCell("EmergencyCareProcedureTime18");}
    public CsvCell getInvestigation19() { return super.getCell("EmergencyCareClinicalInvestigation19");}
    public CsvCell getInvestigationPerformedDate19() { return super.getCell("EmergencyCareProcedureDate19");}
    public CsvCell getInvestigationPerformedTime19() { return super.getCell("EmergencyCareProcedureTime19");}
    public CsvCell getInvestigation20() { return super.getCell("EmergencyCareClinicalInvestigation20");}
    public CsvCell getInvestigationPerformedDate20() { return super.getCell("EmergencyCareProcedureDate20");}
    public CsvCell getInvestigationPerformedTime20() { return super.getCell("EmergencyCareProcedureTime20");}

    //Treatments 1 - 20
    public CsvCell getTreatment(int dataNumber) { return super.getCell("EmergencyCareTreatments"+dataNumber);}
    public CsvCell getTreatmentDate(int dataNumber) { return super.getCell("EmergencyCareTreatmentDate"+dataNumber);}
    public CsvCell getTreatmentTime(int dataNumber) { return super.getCell("EmergencyCareTreatmentTime"+dataNumber);}

    public CsvCell getTreatment1() { return super.getCell("EmergencyCareTreatments1");}
    public CsvCell getTreatmentDate1() { return super.getCell("EmergencyCareTreatmentDate1");}
    public CsvCell getTreatmentTime1() { return super.getCell("EmergencyCareTreatmentTime1");}
    public CsvCell getTreatment2() { return super.getCell("EmergencyCareTreatments2");}
    public CsvCell getTreatmentDate2() { return super.getCell("EmergencyCareTreatmentDate2");}
    public CsvCell getTreatmentTime2() { return super.getCell("EmergencyCareTreatmentTime2");}
    public CsvCell getTreatment3() { return super.getCell("EmergencyCareTreatments3");}
    public CsvCell getTreatmentDate3() { return super.getCell("EmergencyCareTreatmentDate3");}
    public CsvCell getTreatmentTime3() { return super.getCell("EmergencyCareTreatmentTime3");}
    public CsvCell getTreatment4() { return super.getCell("EmergencyCareTreatments4");}
    public CsvCell getTreatmentDate4() { return super.getCell("EmergencyCareTreatmentDate4");}
    public CsvCell getTreatmentTime4() { return super.getCell("EmergencyCareTreatmentTime4");}
    public CsvCell getTreatment5() { return super.getCell("EmergencyCareTreatments5");}
    public CsvCell getTreatmentDate5() { return super.getCell("EmergencyCareTreatmentDate5");}
    public CsvCell getTreatmentTime5() { return super.getCell("EmergencyCareTreatmentTime5");}
    public CsvCell getTreatment6() { return super.getCell("EmergencyCareTreatments6");}
    public CsvCell getTreatmentDate6() { return super.getCell("EmergencyCareTreatmentDate6");}
    public CsvCell getTreatmentTime6() { return super.getCell("EmergencyCareTreatmentTime6");}
    public CsvCell getTreatment7() { return super.getCell("EmergencyCareTreatments7");}
    public CsvCell getTreatmentDate7() { return super.getCell("EmergencyCareTreatmentDate7");}
    public CsvCell getTreatmentTime7() { return super.getCell("EmergencyCareTreatmentTime7");}
    public CsvCell getTreatment8() { return super.getCell("EmergencyCareTreatments8");}
    public CsvCell getTreatmentDate8() { return super.getCell("EmergencyCareTreatmentDate8");}
    public CsvCell getTreatmentTime8() { return super.getCell("EmergencyCareTreatmentTime8");}
    public CsvCell getTreatment9() { return super.getCell("EmergencyCareTreatments9");}
    public CsvCell getTreatmentDate9() { return super.getCell("EmergencyCareTreatmentDate9");}
    public CsvCell getTreatmentTime9() { return super.getCell("EmergencyCareTreatmentTime9");}
    public CsvCell getTreatment10() { return super.getCell("EmergencyCareTreatments10");}
    public CsvCell getTreatmentDate10() { return super.getCell("EmergencyCareTreatmentDate10");}
    public CsvCell getTreatmentTime10() { return super.getCell("EmergencyCareTreatmentTime10");}
    public CsvCell getTreatment11() { return super.getCell("EmergencyCareTreatments11");}
    public CsvCell getTreatmentDate11() { return super.getCell("EmergencyCareTreatmentDate11");}
    public CsvCell getTreatmentTime11() { return super.getCell("EmergencyCareTreatmentTime11");}
    public CsvCell getTreatment12() { return super.getCell("EmergencyCareTreatments12");}
    public CsvCell getTreatmentDate12() { return super.getCell("EmergencyCareTreatmentDate12");}
    public CsvCell getTreatmentTime12() { return super.getCell("EmergencyCareTreatmentTime12");}
    public CsvCell getTreatment13() { return super.getCell("EmergencyCareTreatments13");}
    public CsvCell getTreatmentDate13() { return super.getCell("EmergencyCareTreatmentDate13");}
    public CsvCell getTreatmentTime13() { return super.getCell("EmergencyCareTreatmentTime13");}
    public CsvCell getTreatment14() { return super.getCell("EmergencyCareTreatments14");}
    public CsvCell getTreatmentDate14() { return super.getCell("EmergencyCareTreatmentDate14");}
    public CsvCell getTreatmentTime14() { return super.getCell("EmergencyCareTreatmentTime14");}
    public CsvCell getTreatment15() { return super.getCell("EmergencyCareTreatments15");}
    public CsvCell getTreatmentDate15() { return super.getCell("EmergencyCareTreatmentDate15");}
    public CsvCell getTreatmentTime15() { return super.getCell("EmergencyCareTreatmentTime15");}
    public CsvCell getTreatment16() { return super.getCell("EmergencyCareTreatments16");}
    public CsvCell getTreatmentDate16() { return super.getCell("EmergencyCareTreatmentDate16");}
    public CsvCell getTreatmentTime16() { return super.getCell("EmergencyCareTreatmentTime16");}
    public CsvCell getTreatment17() { return super.getCell("EmergencyCareTreatments17");}
    public CsvCell getTreatmentDate17() { return super.getCell("EmergencyCareTreatmentDate17");}
    public CsvCell getTreatmentTime17() { return super.getCell("EmergencyCareTreatmentTime17");}
    public CsvCell getTreatment18() { return super.getCell("EmergencyCareTreatments18");}
    public CsvCell getTreatmentDate18() { return super.getCell("EmergencyCareTreatmentDate18");}
    public CsvCell getTreatmentTime18() { return super.getCell("EmergencyCareTreatmentTime18");}
    public CsvCell getTreatment19() { return super.getCell("EmergencyCareTreatments19");}
    public CsvCell getTreatmentDate19() { return super.getCell("EmergencyCareTreatmentDate19");}
    public CsvCell getTreatmentTime19() { return super.getCell("EmergencyCareTreatmentTime19");}
    public CsvCell getTreatment20() { return super.getCell("EmergencyCareTreatments20");}
    public CsvCell getTreatmentDate20() { return super.getCell("EmergencyCareTreatmentDate20");}
    public CsvCell getTreatmentTime20() { return super.getCell("EmergencyCareTreatmentTime20");}

    //Referrals 1 - 10
    public CsvCell getReferralToService(int dataNumber) { return super.getCell("ReferredtoServiceSnomed"+dataNumber);}
    public CsvCell getReferralRequestDate(int dataNumber) { return super.getCell("ActivityServiceRequestDate"+dataNumber);}
    public CsvCell getReferralRequestTime(int dataNumber) { return super.getCell("ActivityServiceRequestTime"+dataNumber);}
    public CsvCell getReferralAssessmentDate(int dataNumber) { return super.getCell("ReferraltoServiceAssessmentDate"+dataNumber);}
    public CsvCell getReferralAssessmentTime(int dataNumber) { return super.getCell("ReferraltoServiceAssessmentTime"+dataNumber);}

    public CsvCell getReferralToService1() { return super.getCell("ReferredtoServiceSnomed1");}
    public CsvCell getReferralRequestDate1() { return super.getCell("ActivityServiceRequestDate1");}
    public CsvCell getReferralRequestTime1() { return super.getCell("ActivityServiceRequestTime1");}
    public CsvCell getReferralAssessmentDate1() { return super.getCell("ReferraltoServiceAssessmentDate1");}
    public CsvCell getReferralAssessmentTime1() { return super.getCell("ReferraltoServiceAssessmentTime1");}
    public CsvCell getReferralToService2() { return super.getCell("ReferredtoServiceSnomed2");}
    public CsvCell getReferralRequestDate2() { return super.getCell("ActivityServiceRequestDate2");}
    public CsvCell getReferralRequestTime2() { return super.getCell("ActivityServiceRequestTime2");}
    public CsvCell getReferralAssessmentDate2() { return super.getCell("ReferraltoServiceAssessmentDate2");}
    public CsvCell getReferralAssessmentTime2() { return super.getCell("ReferraltoServiceAssessmentTime2");}
    public CsvCell getReferralToService3() { return super.getCell("ReferredtoServiceSnomed3");}
    public CsvCell getReferralRequestDate3() { return super.getCell("ActivityServiceRequestDate3");}
    public CsvCell getReferralRequestTime3() { return super.getCell("ActivityServiceRequestTime3");}
    public CsvCell getReferralAssessmentDate3() { return super.getCell("ReferraltoServiceAssessmentDate3");}
    public CsvCell getReferralAssessmentTime3() { return super.getCell("ReferraltoServiceAssessmentTime3");}
    public CsvCell getReferralToService4() { return super.getCell("ReferredtoServiceSnomed4");}
    public CsvCell getReferralRequestDate4() { return super.getCell("ActivityServiceRequestDate4");}
    public CsvCell getReferralRequestTime4() { return super.getCell("ActivityServiceRequestTime4");}
    public CsvCell getReferralAssessmentDate4() { return super.getCell("ReferraltoServiceAssessmentDate4");}
    public CsvCell getReferralAssessmentTime4() { return super.getCell("ReferraltoServiceAssessmentTime4");}
    public CsvCell getReferralToService5() { return super.getCell("ReferredtoServiceSnomed5");}
    public CsvCell getReferralRequestDate5() { return super.getCell("ActivityServiceRequestDate5");}
    public CsvCell getReferralRequestTime5() { return super.getCell("ActivityServiceRequestTime5");}
    public CsvCell getReferralAssessmentDate5() { return super.getCell("ReferraltoServiceAssessmentDate5");}
    public CsvCell getReferralAssessmentTime5() { return super.getCell("ReferraltoServiceAssessmentTime5");}
    public CsvCell getReferralToService6() { return super.getCell("ReferredtoServiceSnomed6");}
    public CsvCell getReferralRequestDate6() { return super.getCell("ActivityServiceRequestDate6");}
    public CsvCell getReferralRequestTime6() { return super.getCell("ActivityServiceRequestTime6");}
    public CsvCell getReferralAssessmentDate6() { return super.getCell("ReferraltoServiceAssessmentDate6");}
    public CsvCell getReferralAssessmentTime6() { return super.getCell("ReferraltoServiceAssessmentTime6");}
    public CsvCell getReferralToService7() { return super.getCell("ReferredtoServiceSnomed7");}
    public CsvCell getReferralRequestDate7() { return super.getCell("ActivityServiceRequestDate7");}
    public CsvCell getReferralRequestTime7() { return super.getCell("ActivityServiceRequestTime7");}
    public CsvCell getReferralAssessmentDate7() { return super.getCell("ReferraltoServiceAssessmentDate7");}
    public CsvCell getReferralAssessmentTime7() { return super.getCell("ReferraltoServiceAssessmentTime7");}
    public CsvCell getReferralToService8() { return super.getCell("ReferredtoServiceSnomed8");}
    public CsvCell getReferralRequestDate8() { return super.getCell("ActivityServiceRequestDate8");}
    public CsvCell getReferralRequestTime8() { return super.getCell("ActivityServiceRequestTime8");}
    public CsvCell getReferralAssessmentDate8() { return super.getCell("ReferraltoServiceAssessmentDate8");}
    public CsvCell getReferralAssessmentTime8() { return super.getCell("ReferraltoServiceAssessmentTime8");}
    public CsvCell getReferralToService9() { return super.getCell("ReferredtoServiceSnomed9");}
    public CsvCell getReferralRequestDate9() { return super.getCell("ActivityServiceRequestDate9");}
    public CsvCell getReferralRequestTime9() { return super.getCell("ActivityServiceRequestTime9");}
    public CsvCell getReferralAssessmentDate9() { return super.getCell("ReferraltoServiceAssessmentDate9");}
    public CsvCell getReferralAssessmentTime9() { return super.getCell("ReferraltoServiceAssessmentTime9");}
    public CsvCell getReferralToService10() { return super.getCell("ReferredtoServiceSnomed10");}
    public CsvCell getReferralRequestDate10() { return super.getCell("ActivityServiceRequestDate10");}
    public CsvCell getReferralRequestTime10() { return super.getCell("ActivityServiceRequestTime10");}
    public CsvCell getReferralAssessmentDate10() { return super.getCell("ReferraltoServiceAssessmentDate10");}
    public CsvCell getReferralAssessmentTime10() { return super.getCell("ReferraltoServiceAssessmentTime10");}

    //Safe Guarding Concerns 1 - 10
    public CsvCell getSafeguardingConcern(int dataNumber) { return super.getCell("SafeguardingConcernSnomed"+dataNumber);}

    @Override
    public CsvCell getSafeguardingConcern() {
        throw new IllegalArgumentException("calling wrong version of SusEmergencyCareDataSet, this function relates to V2");
    }

    @Override
    public CsvCell getMHClassificationColumn() {
        throw new IllegalArgumentException("calling wrong version of SusEmergencyCareDataSet, this function relates to V2");
    }

    @Override
    public CsvCell getDiagnosisCol() {
        throw new IllegalArgumentException("calling wrong version of SusEmergencyCareDataSet, this function relates to V2");
    }

    @Override
    public CsvCell getInvestigationCol() {
        throw new IllegalArgumentException("calling wrong version of SusEmergencyCareDataSet, this function relates to V2");
    }

    @Override
    public CsvCell getTreatmentCol() {
        throw new IllegalArgumentException("calling wrong version of SusEmergencyCareDataSet, this function relates to V2");
    }

    @Override
    public CsvCell getReferralToServiceCol() {
        throw new IllegalArgumentException("calling wrong version of SusEmergencyCareDataSet, this function relates to V2");
    }

    @Override
    public CsvCell getAmbulanceCallIdentifier() {
        throw new ArrayIndexOutOfBoundsException("ambulance call identifier not found in v1, use getambulanceincidentnumebr");
    }

    public CsvCell getSafeguardingConcern1() { return super.getCell("SafeguardingConcernSnomed1");}
    public CsvCell getSafeguardingConcern2() { return super.getCell("SafeguardingConcernSnomed2");}
    public CsvCell getSafeguardingConcern3() { return super.getCell("SafeguardingConcernSnomed3");}
    public CsvCell getSafeguardingConcern4() { return super.getCell("SafeguardingConcernSnomed4");}
    public CsvCell getSafeguardingConcern5() { return super.getCell("SafeguardingConcernSnomed5");}
    public CsvCell getSafeguardingConcern6() { return super.getCell("SafeguardingConcernSnomed6");}
    public CsvCell getSafeguardingConcern7() { return super.getCell("SafeguardingConcernSnomed7");}
    public CsvCell getSafeguardingConcern8() { return super.getCell("SafeguardingConcernSnomed8");}
    public CsvCell getSafeguardingConcern9() { return super.getCell("SafeguardingConcernSnomed9");}
    public CsvCell getSafeguardingConcern10() { return super.getCell("SafeguardingConcernSnomed10");}

    @Override
    protected String[] getCsvHeaders(String version) {
        return getCsvHeadersForVersion(version);
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

}