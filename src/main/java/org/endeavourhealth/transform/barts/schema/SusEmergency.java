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

public class SusEmergency extends AbstractFixedParser {
    private static final Logger LOG = LoggerFactory.getLogger(SusEmergency.class);

    public SusEmergency(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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

    public CsvCell getAdditionalecondaryProceduresOPCS() {
        return super.getCell("2nd50thSecondaryProceduresOPCS");
    }

    public CsvCell getCDSRecordType() {
        return super.getCell("CDSRecordType");
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
        return super.getCell("AEStaffMemberCode");
    }

    public CsvCell getLocalPatientId() {
        return super.getCell("LocalPatientID");
    }

    public CsvCell getCdsUniqueId() {
        return super.getCell("CDSUniqueIdentifier");
    }

    public CsvCell getWithheldFlag() {
        return super.getCell("WithheldFlag");
    }


    /*public Date getArrivalDate() throws TransformException {
        return super.getDate("ArrivalDate");
    }

    public Date getArrivalTime() throws TransformException {
        return super.getTime("ArrivalTime");
    }

    public Date getArrivalDateTime() throws TransformException {
        return super.getDateTime("ArrivalDate", "ArrivalTime");
    }

    public Date getDepartureDate() throws TransformException {
        return super.getDate("DepartureDate");
    }

    public Date getDepartureTime() throws TransformException {
        return super.getTime("DepartureTime");
    }

    public Date getDepartureDateTime() throws TransformException {
        return super.getDateTime("DepartureDate", "DepartureTime");
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
//G.P. DETAILS
        ret.add(new FixedParserField("GeneralMedicalPractitionerRegistered", 1023, 8));
        ret.add(new FixedParserField("GPPracticeRegistered", 1031, 12));
//ACCIDENT & EMERGENCY LOCATION GROUP
        ret.add(new FixedParserField("SiteCodeofTreatment", 1043, 12));
//ATTENDANCE DETAILS
        ret.add(new FixedParserField("AttendanceNumber", 1055, 12));
        ret.add(new FixedParserField("ArrivalModeCode", 1067, 1));
        ret.add(new FixedParserField("AttendanceCategoryCode", 1068, 1));
        ret.add(new FixedParserField("AttendanceDisposal", 1069, 2));
        ret.add(new FixedParserField("IncidentLocationType", 1071, 2));
        ret.add(new FixedParserField("PatientGroup", 1073, 2));
        ret.add(new FixedParserField("SourceofReferral", 1075, 2));
        ret.add(new FixedParserField("AEDepartmentType", 1077, 2));
        ret.add(new FixedParserField("ArrivalDate", 1079, 8));
        ret.add(new FixedParserField("ArrivalTime", 1087, 6));
        ret.add(new FixedParserField("AgeAtCDSActivityDate", 1093, 3));
        ret.add(new FixedParserField("OverseasVisitorStatusClassificationAtCDSActivityDate", 1096, 1));
        ret.add(new FixedParserField("InitialAssessmentDate", 1097, 8));
        ret.add(new FixedParserField("InitialAssessmentTime", 1105, 6));
        ret.add(new FixedParserField("DateSeenforTreatment", 1111, 8));
        ret.add(new FixedParserField("TimeSeenforTreatment", 1119, 6));
        ret.add(new FixedParserField("AttendanceConclusionDate", 1125, 8));
        ret.add(new FixedParserField("AttendanceConclusionTime", 1133, 6));
        ret.add(new FixedParserField("DepartureDate", 1139, 8));
        ret.add(new FixedParserField("DepartureTime", 1147, 6));
        ret.add(new FixedParserField("AmbulanceIncidentNumber", 1153, 20));
        ret.add(new FixedParserField("OrganisationCodeConveyingAmbulanceTrust", 1173, 12));
//SERVICE AGREEMENT DETAILS
        ret.add(new FixedParserField("CommissioningSerialNumber", 1185, 6));
        ret.add(new FixedParserField("NHSServiceAgreementLineNumber", 1191, 10));
        ret.add(new FixedParserField("ProviderReferenceNumber", 1201, 17));
        ret.add(new FixedParserField("CommissionerReferenceNumber", 1218, 17));
        ret.add(new FixedParserField("OrganisationCodeCodeofProvider", 1235, 12));
        ret.add(new FixedParserField("OrganisationCodeCodeofCommissioner", 1247, 12));
//PERSON GROUP A& E CONSULTANT
        ret.add(new FixedParserField("AEStaffMemberCode", 1259, 3));
//CLINCIAL DIAGNOSIS (ICD) DETAILS
        ret.add(new FixedParserField("DiagnosisSchemeinUse", 1262, 2));
        ret.add(new FixedParserField("PrimaryDiagnosisICD", 1264, 6));
        ret.add(new FixedParserField("PresentOnAdmissionIndicator", 1270, 1));
        ret.add(new FixedParserField("SecondaryDiagnosisICD", 1271, 6));
        ret.add(new FixedParserField("SecondaryPresentOnAdmissionIndicator", 1277, 1));
        ret.add(new FixedParserField("2nd50thSecondaryDiagnosisICD", 1278, 343));
//CLINICAL DIAGNOSIS (READ) DETAILS
        /*ret.add(new FixedParserField("DiagnosisSchemeinUse", 1621, 2));
        ret.add(new FixedParserField("PrimaryDiagnosisRead", 1623, 5));
        ret.add(new FixedParserField("SecondaryDiagnosisRead150", 1628, 250));*/
//A & E DIAGNOSIS DETAILS
        ret.add(new FixedParserField("AEDiagnosisSchemeinUse", 1878, 2));
        ret.add(new FixedParserField("PrimaryAEDiagnosisGroup", 1880, 6));
        ret.add(new FixedParserField("PrimaryAEDiagnosis", 1880, 6));
        ret.add(new FixedParserField("1st50thSecondaryDiagnosisGroups", 1886, 300));
//A & E INVESTIGATION DETAILS
        ret.add(new FixedParserField("InvestigationSchemeinUse", 2186, 2));
        ret.add(new FixedParserField("PrimaryInvestigationGroup", 2188, 6));
        ret.add(new FixedParserField("PrimaryInvestigation", 2188, 6));
        ret.add(new FixedParserField("1st50thSecondaryInvestigationGroups", 2194, 300));
//CLINICAL TREATMENT (OPCS) DETAILS
        ret.add(new FixedParserField("ProcedureSchemeInUse", 2494, 2));
        ret.add(new FixedParserField("PrimaryProcedureOPCS", 2496, 4));
        ret.add(new FixedParserField("PrimaryProcedureDate", 2500, 8));
        ret.add(new FixedParserField("PrimaryMainOperatingHCPRegistrationIssuerCode", 2508, 2));
        ret.add(new FixedParserField("PrimaryMainOperatingHCPRegistrationEntryIdentifier", 2510, 12));
        ret.add(new FixedParserField("PrimaryResponsibleAnaesthetistRegistrationIssuerCode", 2522, 2));
        ret.add(new FixedParserField("PrimaryResponsibleAnaesthetistRegistrationEntryIdentifier", 2524, 12));
        ret.add(new FixedParserField("SecondaryProcedureOPCS", 2536, 4));
        ret.add(new FixedParserField("SecondaryProcedureDate", 2540, 8));
        ret.add(new FixedParserField("SecondaryMainOperatingHCPRegistrationIssuerCode", 2548, 2));
        ret.add(new FixedParserField("SecondaryMainOperatingHCPRegistrationEntryIdentifier", 2550, 12));
        ret.add(new FixedParserField("SecondaryResponsibleAnaesthetistRegistrationIssuerCode", 2562, 2));
        ret.add(new FixedParserField("SecondaryResponsibleAnaesthetistRegistrationEntryIdentifier", 2564, 12));
        ret.add(new FixedParserField("2nd50thSecondaryProceduresOPCS", 2576, 1960));
//CLINICAL TREATMENT (READ) DETAILS
        /*ret.add(new FixedParserField("ProcedureSchemeInUse", 4536, 2));
        ret.add(new FixedParserField("PrimaryProcedureGroupREAD", 4538, 13));
        ret.add(new FixedParserField("PrimaryProcedureREAD", 4538, 5));
        ret.add(new FixedParserField("PrimaryProcedureDate", 4543, 8));
        ret.add(new FixedParserField("1stSecondaryProcedureGroupREAD", 4551, 13));
        ret.add(new FixedParserField("SecondaryProcedureREAD", 4551, 5));
        ret.add(new FixedParserField("SecondaryProcedureDate", 4556, 8));
        ret.add(new FixedParserField("2nd50thSecondaryProceduresREAD", 4564, 637));*/
//A & E TREATMENT DETAILS
        ret.add(new FixedParserField("TreatmentSchemeinUse", 5201, 2));
        ret.add(new FixedParserField("PrimaryTreatmentGroup", 5203, 14));
        ret.add(new FixedParserField("PrimaryTreatmentCode", 5203, 6));
        ret.add(new FixedParserField("PrimaryTreatmentDate", 5209, 8));
        ret.add(new FixedParserField("1stSecondaryTreatmentGroup", 5217, 14));
        ret.add(new FixedParserField("SecondaryTreatmentCode", 5217, 6));
        ret.add(new FixedParserField("SecondaryTreatmentDate", 5223, 8));
        ret.add(new FixedParserField("2nd50thSecondaryTreatment", 5231, 686));


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

        ret.add(new FixedParserField("GP",    1023, 8));
        ret.add(new FixedParserField("GPPractice",    1031, 12));

        ret.add(new FixedParserField("ArrivalDate",    1079, 8));
        ret.add(new FixedParserField("ArrivalTime",    1087, 6));
        ret.add(new FixedParserField("DepartureDate",    1139, 8));
        ret.add(new FixedParserField("DepartureTime",    1147, 6));

        ret.add(new FixedParserField("StaffCode",    1259, 3));

        ret.add(new FixedParserField("ICDPrimaryDiagnosis",    1264, 6));
        ret.add(new FixedParserField("ICDSecondaryDiagnosisList",    1271, 350));

        ret.add(new FixedParserField("OPCSPrimaryProcedureCode",    2496, 4));
        ret.add(new FixedParserField("OPCSPrimaryProcedureDate",    2500, 8));
        ret.add(new FixedParserField("OPCSecondaryProcedureList",    2536, 2000));

        return ret;
    }*/
}