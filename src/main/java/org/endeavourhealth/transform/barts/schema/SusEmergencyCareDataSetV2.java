package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;
import static org.endeavourhealth.transform.barts.schema.EmergencyCareDataSetStatics.*;

public class SusEmergencyCareDataSetV2 extends AbstractCsvParser implements CdsRecordEmergencyCDSI {
    private static final Logger LOG = LoggerFactory.getLogger(SusEmergencyCareDataSetV2.class);

    public SusEmergencyCareDataSetV2(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        //confusingly the ECDS file is a pipe-delimited file, but uses the CDS date and time formats,
        //plus the Tails file is fixed width rather than delimited
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT_ECDS_V2.withHeader(getCsvHeadersForVersion(version)), //no headers in the file
                BartsCsvToFhirTransformer.CDS_DATE_FORMAT,
                BartsCsvToFhirTransformer.CDS_TIME_FORMAT);
    }

    private static String[] getCsvHeadersForVersion(String version) {
        return V2_DATA_SET;
    }

    public CsvCell getCdsUniqueId() {
        return super.getCell("CDSUniqueIdentifier");
    }

    public CsvCell getLocalPatientId() {
        return super.getCell("LocalPatientID");
    }

    public CsvCell getNhsNumber() {
        return super.getCell("NHSNumber");
    }

    public CsvCell getCdsActivityDate() {
        return super.getCell("CDSActivityDate");
    }

    public CsvCell getCdsUpdateType() {
        return super.getCell("CDSUpdateType");
    }

    public CsvCell getPersonBirthDate() {
        return super.getCell("DateofBirth");
    }

    public CsvCell getWithheldReason() {
        return super.getCell("WithheldReason");
    }

    public CsvCell getPatientPathwayIdentifier() {
        return super.getCell("PatientPathwayIdentifier");
    }

    public CsvCell getDepartmentType() {
        return super.getCell("EmergencyCareDepartmentType");
    }

    public CsvCell getAmbulanceIncidentNumber() {
        throw new ArrayIndexOutOfBoundsException("ambulance incident number not found in V2 dataset use get ambulancecallidentifier");
    }

    public CsvCell getAmbulanceCallIdentifier(){
        return super.getCell("AmbulanceCallIdentifier");
    }

    public CsvCell getTreatmentOrganisationCode() {
        return super.getCell("OrganisationSiteIdentifierofTreatment");
    }

    public CsvCell getAttendanceIdentifier() {
        return super.getCell("EmergencyCareAttendanceIdentifier");
    }

    public CsvCell getArrivalMode() {
        return super.getCell("EmergencyCareArrivalMode");
    }

    public CsvCell getAttendanceCategory() {
        return super.getCell("EmergencyCareAttendanceCategory");
    }

    public CsvCell getAttendanceSource() {
        return super.getCell("EmergencyCareAttendanceSource");
    }

    public CsvCell getArrivalDate() {
        return super.getCell("EmergencyCareArrivalDate");
    }

    public CsvCell getArrivalTime() {
        return super.getCell("EmergencyCareArrivalTime");
    }

    public CsvCell getInitialAssessmentDate() {
        return super.getCell("EmergencyCareInitialAssessmentDate");
    }

    public CsvCell getInitialAssessmentTime() {
        return super.getCell("EmergencyCareInitialAssessmentTime");
    }

    public CsvCell getChiefComplaint() {
        return super.getCell("EmergencyCareChiefComplaint");
    }

    public CsvCell getDateSeenforTreatment() {
        return super.getCell("EmergencyCareDateSeenforTreatment");
    }

    public CsvCell getTimeSeenforTreatment() {
        return super.getCell("EmergencyCareTimeSeenforTreatment");
    }

    public CsvCell getDecidedtoAdmitDate() {
        return super.getCell("DecidedtoAdmitDate");
    }

    public CsvCell getDecidedtoAdmitTime() {
        return super.getCell("DecidedtoAdmitTime");
    }

    public CsvCell getActivityTreatmentFunctionCode() {
        return super.getCell("ActivityTreatmentFunctionCode");
    }

    public CsvCell getDischargeStatus() {
        return super.getCell("EmergencyCareDischargeStatusSnomed");
    }

    public CsvCell getConclusionDate() {
        return super.getCell("EmergencyCareConclusionDate");
    }

    public CsvCell getConclusionTime() {
        return super.getCell("EmergencyCareConclusionTime");
    }

    public CsvCell getDepartureDate() {
        return super.getCell("EmergencyCareDepartureDate");
    }

    public CsvCell getDepartureTime() {
        return super.getCell("EmergencyCareDepartureTime");
    }

    public CsvCell getDischargeDestination() {
        return super.getCell("EmergencyCareDischargeDestination");
    }

    public CsvCell getDischargeDestinationSiteId() {
        return super.getCell("OrganisationSiteIdentifierDischargeFromEmergencyCare");
    }

    public CsvCell getDischargeFollowUp() {
        return super.getCell("EmergencyCareDischargeFollowup");
    }

    //mental health classifications 1-10
    public CsvCell getMHClassificationColumn(){
        return super.getCell("MHALegalStatusColumn");
    }
    public CsvCell getMHClassificationCode(int dataNumber) {
        return super.getCell("MHALegalStatusClassificationCode" + dataNumber);
    }

    public CsvCell getMHClassificationStartDate(int dataNumber) {
        return super.getCell("StartDateMHALegalStatusClassificationAssignmentPeriod" + dataNumber);
    }

    public CsvCell getMHClassificationStartTime(int dataNumber) {
        return super.getCell("StartTimeMHALegalStatusClassificationAssignmentPeriod" + dataNumber);
    }

    public CsvCell getMHClassificationEndDate(int dataNumber) {
        return super.getCell("ExpiryDateMHALegalStatusClassification" + dataNumber);
    }

    public CsvCell getMHClassificationEndTime(int dataNumber) {
        return super.getCell("ExpiryTimeMHALegalStatusClassification" + dataNumber);
    }
    public CsvCell getDiagnosisCol(){return super.getCell("EmergencyCareDiagnosis");}
    //Diagnosis 1 - 20
    public CsvCell getDiagnosis(int dataNumber) {
        return super.getCell("EmergencyCareDiagnosis" + dataNumber);
    }

    public CsvCell getDiagnosisQualifier(int dataNumber) {
        return super.getCell("EmergencyCareDiagnosisQualifier" + dataNumber);
    }

    @Override
    public CsvCell getInvestigation(int dataNumber) {
        throw new IllegalArgumentException("CALLING OUT OF DATE getInvestigation function");
    }

    //Investigations 1 - 20 (note use of ProcedureDate etc. for date column)
    public CsvCell getInvestigationCol() {
        return super.getCell("EmergencyCareClinicalInvestigation");
    }

    public CsvCell getInvestigationPerformedDate(int dataNumber) {
        return super.getCell("EmergencyCareProcedureDate" + dataNumber);
    }

    public CsvCell getInvestigationPerformedTime(int dataNumber) {
        return super.getCell("EmergencyCareProcedureTime" + dataNumber);
    }

    @Override
    public CsvCell getTreatment(int dataNumber) {
        throw new IllegalArgumentException("CALLING OUT OF DATE getTreatment function");
    }

    @Override
    public CsvCell getTreatmentDate(int dataNumber) {
        throw new IllegalArgumentException("CALLING OUT OF DATE getTreatmentDate function");
    }

    @Override
    public CsvCell getTreatmentTime(int dataNumber) {
        throw new IllegalArgumentException("CALLING OUT OF DATE getTreatmentTime function");
    }

    @Override
    public CsvCell getReferralToService(int dataNumber) {
        throw new IllegalArgumentException("CALLING OUT OF DATE getReferralToService function");
    }

    //Treatments 1 - 20
    public CsvCell getTreatmentCol() {
        return super.getCell("EmergencyCareTreatments");
    }

    public CsvCell getReferralToServiceCol(){
        return super.getCell("ReferredtoService");
    }

    public CsvCell getReferralRequestDate(int dataNumber) {
        return super.getCell("ActivityServiceRequestDate" + dataNumber);
    }

    public CsvCell getReferralRequestTime(int dataNumber) {
        return super.getCell("ActivityServiceRequestTime" + dataNumber);
    }

    public CsvCell getReferralAssessmentDate(int dataNumber) {
        return super.getCell("ReferraltoServiceAssessmentDate" + dataNumber);
    }

    public CsvCell getReferralAssessmentTime(int dataNumber) {
        return super.getCell("ReferraltoServiceAssessmentTime" + dataNumber);
    }

    @Override
    public CsvCell getSafeguardingConcern(int dataNumber) {
        throw new IllegalArgumentException("CALLING OUT OF DATE getSafeGuardingConcern function");
    }

    public CsvCell getCodedScoreAssessmentCol(){
        return super.getCell("CodedAssessmentToolTypeSNOMED");
    }
    public CsvCell getCodedClinicalObservationsCol(){
        return super.getCell("CodedObservationSNOMED");
    }
    public CsvCell getCodedClinicalFindingsCol(){
        return super.getCell("CodedFindingSNOMED");
    }

    //Safe Guarding Concerns 1 - 10
    public CsvCell getSafeguardingConcern() {
        return super.getCell("SafeguardingConcern");
    }

    public String getRepeatingGroupColumnContents(String columnName){
        return super.getCell(columnName).getString();
    }


    public CsvCell getDiseaseOutbreakNotificationDescription(){return super.getCell("DiseaseOutbreakNotificationDescription");}
    public CsvCell getEmergencyCareClinicallyReadyToProceedTimestamp(){return super.getCell("EmergencyCareClinicallyReadytoProceedTimestamp");}

    public CsvCell getActivityAlcoholAndDrugInvolvementSNOMED(){return super.getCell("InjuryCharacteristicsActivityAlcoholandDrugInvolvementCol");}
    public CsvCell getAssaultLocationDescription(){return super.getCell("AssaultLocationDescription");}


    @Override
    protected String[] getCsvHeaders(String version) {
        return getCsvHeadersForVersion(version);
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

}