package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractFixedParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FixedParserField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SusOutpatient extends AbstractFixedParser implements CdsRecordI, CdsRecordOutpatientI {
    private static final Logger LOG = LoggerFactory.getLogger(SusOutpatient.class);

    public SusOutpatient(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, BartsCsvToFhirTransformer.CDS_DATE_FORMAT, BartsCsvToFhirTransformer.CDS_TIME_FORMAT);
    }


    public CsvCell getProcedureSchemeInUse() {
        return super.getCell("ProcedureSchemeInUse");
    }


    public CsvCell getPrimaryProcedureOPCS() {
        return super.getCell("PrimaryProcedureOPCS");
    }

    public CsvCell getPrimaryProcedureDate() {
        return super.getCell("PrimaryProcedureDate");
    }


    public CsvCell getSecondaryProcedureOPCS() {
        return super.getCell("SecondaryProcedureOPCS");
    }

    public CsvCell getSecondaryProcedureDate() {
        return super.getCell("SecondaryProcedureDate");
    }

    public CsvCell getAdditionalSecondaryProceduresOPCS() {
        return super.getCell("2nd50thSecondaryProceduresOPCS");
    }

    public CsvCell getCDSRecordType() {
        return super.getCell("CDSRecordType");
    }

    public CsvCell getCdsUniqueId() {
        return super.getCell("CDSUniqueIdentifier");
    }

    public CsvCell getPatientLocalId() {
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
        return super.getCell("PersonBirthDate");
    }

    public CsvCell getConsultantCode() {
        return super.getCell("ConsultantCode");
    }

    public CsvCell getLocalPatientId() {
        return super.getCell("LocalPatientID");
    }

    public CsvCell getWithheldFlag() {
        return super.getCell("WithheldFlag");
    }

    public CsvCell getPrimaryDiagnosisICD() {return super.getCell("PrimaryDiagnosisICD"); }
    public CsvCell getSecondaryDiagnosisICD() {return super.getCell("SecondaryDiagnosisICD");}
    public CsvCell getAdditionalSecondaryDiagnosisICD() {return super.getCell("2nd50thSecondaryDiagnosisICD");}

    public CsvCell getPatientPathwayIdentifier() {
        return super.getCell("PatientPathwayIdentifier");
    }

    public CsvCell getAttendanceIdentifier() {
        return super.getCell("AttendanceIdentifier");
    }

    public CsvCell getAdministrativeCategoryCode() {
        return super.getCell("AdministrativeCategoryCode");
    }

    public CsvCell getAppointmentAttendedCode() {
        return super.getCell("AttendedOrDidNotAttendCode");
    }

    public CsvCell getAppointmentOutcomeCode() {
        return super.getCell("OutcomeofAttendanceCode");
    }

    public CsvCell getAppointmentDate() {
        return super.getCell("AppointmentDate");
    }

    public CsvCell getAppointmentTime() {
        return super.getCell("AppointmentTime");
    }

    public CsvCell getAppointmentSiteCode() {
        return super.getCell("SiteCodeofTreatment");
    }



    /*public String getConsultantCode() {
        return super.getString("ConsultantCode");
    }

    public String getOutcomeCode() {
        return super.getString("OutcomeofAttendanceCode");
    }

    public Date getAppointmentDate() throws TransformException {
        return super.getDate("AppointmentDate");
    }

    public Date getAppointmentTime() throws TransformException {
        return super.getTime("AppointmentTime");
    }

    public Date getAppointmentDateTime() throws TransformException {
        return super.getDateTime("AppointmentDate", "AppointmentTime");
    }

    public int getExpectedDurationMinutes() throws TransformException {
        return super.getInt("ExpectedDurationOfAppointment");
    }

    public Date getExpectedLeavingDateTime() throws TransformException {
        return new Date(getAppointmentDateTime().getTime() + (getExpectedDurationMinutes() * 60000));
    }*/

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    @Override
    protected boolean skipFirstRow() {
        return false;
    }

    @Override
    protected List<FixedParserField> getFieldList(String version) {

        List<FixedParserField> ret = new ArrayList<>();

        //file definition in Indigo 4 Standard BT Translation Service v6-2 Specification - v1.0.1.xls

        //NOTE the below list was generated from the inpatient CDS specification spreadsheet, and includes all columns
        //although columns we know aren't needed have been commented out

        //RECORD HEADER INFORMATION
        ret.add(new FixedParserField("CDSVersion", 1, 6));
        ret.add(new FixedParserField("CDSRecordType", 7, 3));
        ret.add(new FixedParserField("CDSBulkReplacementGroup", 10, 3));
        ret.add(new FixedParserField("CDSProtocolIdentifier", 13, 3));
        ret.add(new FixedParserField("CDSUniqueIdentifier", 16, 35));
        ret.add(new FixedParserField("CDSUpdateType", 51, 1));
        ret.add(new FixedParserField("CDSApplicableDate", 52, 8));
        ret.add(new FixedParserField("CDSApplicableTime", 60, 6));
        ret.add(new FixedParserField("CDSExtractDate", 66, 8));
        ret.add(new FixedParserField("CDSExtractTime", 74, 6));
        ret.add(new FixedParserField("CDSReportPeriodStartDate", 80, 8));
        ret.add(new FixedParserField("CDSReportPeriodEndDate", 88, 8));
        ret.add(new FixedParserField("CDSCensusDate", 96, 8));
        ret.add(new FixedParserField("CDSActivityDate", 104, 8));
        ret.add(new FixedParserField("CDSSenderIdentity", 112, 12));
        /*ret.add(new FixedParserField("CDSPrimaryRecipientIdentity", 124, 12));
        ret.add(new FixedParserField("CDSCopyRecipientIdentity1", 136, 12));
        ret.add(new FixedParserField("CDSCopyRecipientIdentity2", 148, 12));
        ret.add(new FixedParserField("CDSCopyRecipientIdentity3", 160, 12));
        ret.add(new FixedParserField("CDSCopyRecipientIdentity4", 172, 12));
        ret.add(new FixedParserField("CDSCopyRecipientIdentity5", 184, 12));
        ret.add(new FixedParserField("CDSCopyRecipientIdentity6", 196, 12));
        ret.add(new FixedParserField("CDSCopyRecipientIdentity7", 208, 12));*/
//PATIENT PATHWAY
        ret.add(new FixedParserField("UniqueBookingReferenceNumberConverted", 220, 12));
        ret.add(new FixedParserField("PatientPathwayIdentifier", 232, 20));
        ret.add(new FixedParserField("OrganisationCodeofthePatientPathwayIdentifier", 252, 12));
//RTT PERIOD CHARACTERISTICS
        ret.add(new FixedParserField("ReferralToTreatmentPeriodStatus", 264, 2));
        ret.add(new FixedParserField("WaitingTimeMeasurementType", 266, 2));
        ret.add(new FixedParserField("ReferralToTreatmentPeriodStartDate", 268, 8));
        ret.add(new FixedParserField("ReferralToTreatmentPeriodEndDate", 276, 8));
//PATIENT IDENTITY
        ret.add(new FixedParserField("LocalPatientID", 284, 10));
        ret.add(new FixedParserField("OrganisationCodeLocalPatientID", 294, 12));
        ret.add(new FixedParserField("NHSNumberStatusIndicator", 306, 2));
        ret.add(new FixedParserField("NHSNumber", 308, 10));
        ret.add(new FixedParserField("WithheldFlag", 318, 1));
        ret.add(new FixedParserField("WithheldIdentityReason", 319, 2));
        ret.add(new FixedParserField("PersonBirthDate", 321, 8));
//PATIENT NAME
        ret.add(new FixedParserField("PatientNameType", 329, 2));
        ret.add(new FixedParserField("PatientFullName", 331, 70));
        ret.add(new FixedParserField("PatientRequestedName", 401, 70));
        ret.add(new FixedParserField("PatientTitle", 471, 35));
        ret.add(new FixedParserField("PatientForename", 506, 35));
        ret.add(new FixedParserField("PatientSurname", 541, 35));
        ret.add(new FixedParserField("PatientNameSuffix", 576, 35));
        ret.add(new FixedParserField("PatientInitials", 611, 35));
//PATIENT ADDRESS
        ret.add(new FixedParserField("PatientAddressType", 646, 2));
        ret.add(new FixedParserField("PatientUnstructuredAddress", 648, 175));
        ret.add(new FixedParserField("PatientAddressStructured1", 823, 35));
        ret.add(new FixedParserField("PatientAddressStructured2", 858, 35));
        ret.add(new FixedParserField("PatientAddressStructured3", 893, 35));
        ret.add(new FixedParserField("PatientAddressStructured4", 928, 35));
        ret.add(new FixedParserField("PatientAddressStructured5", 963, 35));
        ret.add(new FixedParserField("Postcode", 998, 8));
//PATIENT ORGANISATION RESIDENCE
        ret.add(new FixedParserField("OrganisationCodeResidenceResponsibility", 1006, 12));
//PATIENT CHARACTERISTICS
        ret.add(new FixedParserField("PersonCurrentGender", 1018, 1));
        ret.add(new FixedParserField("CarerSupportIndicator", 1019, 2));
        ret.add(new FixedParserField("EthnicCategory", 1021, 2));
//PERSON GROUP CONSULTANT
        ret.add(new FixedParserField("ConsultantCode", 1023, 8));
        ret.add(new FixedParserField("CareProfessionalMainSpecialtyCode", 1031, 3));
        ret.add(new FixedParserField("ActivityTreatmentFunctionCode", 1034, 3));
        ret.add(new FixedParserField("LocalSubSpecialtyCode", 1037, 8));
//CLINICAL (ICD) DETAILS
        ret.add(new FixedParserField("DiagnosisSchemeinUse", 1045, 2));
        ret.add(new FixedParserField("PrimaryDiagnosisICD", 1047, 6));
        ret.add(new FixedParserField("PresentOnAdmissionIndicator", 1053, 1));
        ret.add(new FixedParserField("SecondaryDiagnosisICD", 1054, 6));
        ret.add(new FixedParserField("SecondaryPresentOnAdmissionIndicator", 1060, 1));
        ret.add(new FixedParserField("2nd50thSecondaryDiagnosisICD", 1061, 343));
//CLINICAL (READ) DETAILS
        /*ret.add(new FixedParserField("DiagnosisSchemeinUse", 1404, 2));
        ret.add(new FixedParserField("PrimaryDiagnosisRead", 1406, 5));
        ret.add(new FixedParserField("SecondaryDiagnosisRead150", 1411, 250));*/
//ACTIVITY CHARACTERISTICS
        ret.add(new FixedParserField("AttendanceIdentifier", 1661, 12));
        ret.add(new FixedParserField("AdministrativeCategoryCode", 1673, 2));
        ret.add(new FixedParserField("AttendedOrDidNotAttendCode", 1675, 1));
        ret.add(new FixedParserField("FirstAttendanceCode", 1676, 1));
        ret.add(new FixedParserField("MedicalStaffTypeSeeingPatient", 1677, 2));
        ret.add(new FixedParserField("OperationStatusCode", 1679, 1));
        ret.add(new FixedParserField("OutcomeofAttendanceCode", 1680, 1));
        ret.add(new FixedParserField("AppointmentDate", 1681, 8));
        ret.add(new FixedParserField("AppointmentTime", 1689, 6));
        ret.add(new FixedParserField("ExpectedDurationOfAppointment", 1695, 3));
        ret.add(new FixedParserField("AgeAtCDSActivityDate", 1698, 3));
        ret.add(new FixedParserField("OverseasVisitorStatusClassificationAtCDSActivityDate", 1701, 1));
        ret.add(new FixedParserField("EarliestReasonableOfferDate", 1702, 8));
        ret.add(new FixedParserField("EarliestClinicallyAppropriateDate", 1710, 8));
        ret.add(new FixedParserField("ConsultationMediumUsed", 1718, 2));
        ret.add(new FixedParserField("MultiProfessionalorMultidisciplinaryConsultationIndicationCode", 1720, 1));
        ret.add(new FixedParserField("RehabilitationAssessmentTeamType", 1721, 1));
//SERVICE AGREEMENT DETAILS
        ret.add(new FixedParserField("CommissioningSerialNumber", 1722, 6));
        ret.add(new FixedParserField("NHSServiceAgreementLineNumber", 1728, 10));
        ret.add(new FixedParserField("ProviderReferenceNumber", 1738, 17));
        ret.add(new FixedParserField("CommissionerReferenceNumber", 1755, 17));
        ret.add(new FixedParserField("OrganisationCodeCodeofProvider", 1772, 12));
        ret.add(new FixedParserField("OrganisationCodeCodeofCommissioner", 1784, 12));
//CLINICAL TREATMENT (OPCS) DETAILS
        ret.add(new FixedParserField("ProcedureSchemeInUse", 1796, 2));
        ret.add(new FixedParserField("PrimaryProcedureOPCS", 1798, 4));
        ret.add(new FixedParserField("PrimaryProcedureDate", 1802, 8));
        ret.add(new FixedParserField("PrimaryMainOperatingHCPRegistrationIssuerCode", 1810, 2));
        ret.add(new FixedParserField("PrimaryMainOperatingHCPRegistrationEntryIdentifier", 1812, 12));
        ret.add(new FixedParserField("PrimaryResponsibleAnaesthetistRegistrationIssuerCode", 1824, 2));
        ret.add(new FixedParserField("PrimaryResponsibleAnaesthetistRegistrationEntryIdentifier", 1826, 12));
        ret.add(new FixedParserField("SecondaryProcedureOPCS", 1838, 4));
        ret.add(new FixedParserField("SecondaryProcedureDate", 1842, 8));
        ret.add(new FixedParserField("SecondaryMainOperatingHCPRegistrationIssuerCode", 1850, 2));
        ret.add(new FixedParserField("SecondaryMainOperatingHCPRegistrationEntryIdentifier", 1852, 12));
        ret.add(new FixedParserField("SecondaryResponsibleAnaesthetistRegistrationIssuerCode", 1864, 2));
        ret.add(new FixedParserField("SecondaryResponsibleAnaesthetistRegistrationEntryIdentifier", 1866, 12));
        ret.add(new FixedParserField("2nd50thSecondaryProceduresOPCS", 1878, 1960));
//CLINICAL TREATMENT (READ) DETAILS
        /*ret.add(new FixedParserField("ProcedureSchemeInUse", 3838, 2));
        ret.add(new FixedParserField("PrimaryProcedureGroupREAD", 3840, 13));
        ret.add(new FixedParserField("PrimaryProcedureREAD", 3840, 5));
        ret.add(new FixedParserField("PrimaryProcedureDate", 3845, 8));
        ret.add(new FixedParserField("1stSecondaryProcedureGroupREAD", 3853, 13));
        ret.add(new FixedParserField("SecondaryProcedureREAD", 3853, 5));
        ret.add(new FixedParserField("SecondaryProcedureDate", 3858, 8));
        ret.add(new FixedParserField("2nd50thSecondaryProceduresREAD", 3866, 637));*/
//ATTENDANCE LOCATION GROUP
        ret.add(new FixedParserField("LocationClass", 4503, 2));
        ret.add(new FixedParserField("SiteCodeofTreatment", 4505, 12));
        ret.add(new FixedParserField("ActivityLocationType", 4517, 3));
        ret.add(new FixedParserField("ClinicCode", 4520, 12));
//G.P. DETAILS
        ret.add(new FixedParserField("GeneralMedicalPractitionerRegistered", 4532, 8));
        ret.add(new FixedParserField("GPPracticeRegistered", 4540, 12));
//REFERRAL DETAILS
        ret.add(new FixedParserField("PriorityTypeCode", 4552, 1));
        ret.add(new FixedParserField("ServiceTypeRequestedCode", 4553, 1));
        ret.add(new FixedParserField("SourceOfReferralOutpatients", 4554, 2));
        ret.add(new FixedParserField("ReferralRequestReceivedDate", 4556, 8));
        ret.add(new FixedParserField("DirectAccessReferralIndicator", 4564, 1));
        ret.add(new FixedParserField("ReferrerCode", 4565, 8));
        ret.add(new FixedParserField("ReferringOrganisationCode", 4573, 12));
//D.N.A. DETAILS
        ret.add(new FixedParserField("LastDNAorPatientCancelledDate", 4585, 8));


        return ret;
    }

    /*@Override
    protected List<FixedParserField> getFieldList(String version) {

        List<FixedParserField> ret = new ArrayList<>();

        ret.add(new FixedParserField("CDSVersion",             1, 6));
        ret.add(new FixedParserField("CDSRecordType",          7, 3));
        ret.add(new FixedParserField("CDSReplacementgroup",    10, 3));
        ret.add(new FixedParserField("CDSUniqueID",    16, 35));
        ret.add(new FixedParserField("CDSUpdateType",    51, 1));

        ret.add(new FixedParserField("MRN",    284, 10));
        ret.add(new FixedParserField("NHSNo",    308, 10));
        ret.add(new FixedParserField("DOB",    321, 8));
        ret.add(new FixedParserField("PatientTitle",    471, 35));
        ret.add(new FixedParserField("PatientForename",    506, 35));
        ret.add(new FixedParserField("PatientSurname",    541, 35));

        ret.add(new FixedParserField("AddressType",    646, 2));
        ret.add(new FixedParserField("UnstructuredAddress",    648, 175));
        ret.add(new FixedParserField("Address1",    823, 35));
        ret.add(new FixedParserField("Address2",    858, 35));
        ret.add(new FixedParserField("Address3",    893, 35));
        ret.add(new FixedParserField("Address4",    928, 35));
        ret.add(new FixedParserField("Address5",    963, 35));
        ret.add(new FixedParserField("PostCode",    998, 8));

        ret.add(new FixedParserField("Gender",    1018, 1));
        ret.add(new FixedParserField("EthnicCategory",    1021, 2));

        ret.add(new FixedParserField("ConsultantCode",    1023, 8));

        ret.add(new FixedParserField("ICDPrimaryDiagnosis",    1047, 6));
        ret.add(new FixedParserField("ICDSecondaryDiagnosisList",    1054, 350));

        // 1	Discharged from CONSULTANT's care (last attendance), 2	Another APPOINTMENT given, 3	APPOINTMENT to be made at a later date
        ret.add(new FixedParserField("OutcomeCode",    1680, 1));
        ret.add(new FixedParserField("AppointmentDate",    1681, 8));
        ret.add(new FixedParserField("AppointmentTime",    1689, 6));
        ret.add(new FixedParserField("ExpectedDurationMinutes",    1695, 3));

        ret.add(new FixedParserField("OPCSPrimaryProcedureCode",    1798, 4));
        ret.add(new FixedParserField("OPCSPrimaryProcedureDate",    1802, 8));
        ret.add(new FixedParserField("OPCSecondaryProcedureList",    1838, 2000));


        ret.add(new FixedParserField("GP",    4532, 8));
        ret.add(new FixedParserField("GPPractice",    4540, 12));
        
        return ret;
    }*/
}