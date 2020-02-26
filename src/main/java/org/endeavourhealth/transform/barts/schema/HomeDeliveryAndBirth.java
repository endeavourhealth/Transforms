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

public class HomeDeliveryAndBirth extends AbstractFixedParser implements CdsRecordI, CdsRecordInpatientI {
    private static final Logger LOG = LoggerFactory.getLogger(HomeDeliveryAndBirth.class);

    public HomeDeliveryAndBirth(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, BartsCsvToFhirTransformer.CDS_DATE_FORMAT, BartsCsvToFhirTransformer.CDS_TIME_FORMAT);
    }

    public CsvCell getCdsUniqueId() {
        return super.getCell("CDSUniqueIdentifier");
    }

    public CsvCell getCDSRecordType()  { return super.getCell("CDSRecordType"); }

    public CsvCell getLocalPatientId() { return super.getCell("LocalPatientID");}

    public CsvCell getNhsNumber() { return super.getCell("NHSNumber");}

    public CsvCell getCdsActivityDate() {return super.getCell("CDSActivityDate");}

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

    public CsvCell getCdsUpdateType() { return super.getCell("CDSUpdateType");}
    public CsvCell getPersonBirthDate() { return super.getCell("PersonBirthDate");}
    public CsvCell getConsultantCode() { return super.getCell("ConsultantCode");}
    public CsvCell getWithheldFlag() { return super.getCell("WithheldFlag");}

    public CsvCell getPrimaryDiagnosisICD() {return super.getCell("PrimaryDiagnosisICD"); }
    public CsvCell getSecondaryDiagnosisICD() {return super.getCell("SecondaryDiagnosisICD");}
    public CsvCell getAdditionalSecondaryDiagnosisICD() {return super.getCell("2nd50thSecondaryDiagnosisICD");}

    public CsvCell getPatientPathwayIdentifier() {
        return super.getCell("PatientPathwayIdentifier");
    }

    public CsvCell getHospitalSpellStartDate() {
        return super.getCell("StartDateHospitalProviderSpell");
    }

    public CsvCell getHospitalSpellStartTime() {
        return super.getCell("StartTimeHospitalProviderSpell");
    }

    public CsvCell getHospitalSpellNumber() {
        return super.getCell("HospitalProviderSpellNumber");
    }

    public CsvCell getAdmissionMethodCode() {
        return super.getCell("AdmissionMethodCode");
    }

    public CsvCell getAdmissionSourceCode() {
        return super.getCell("SourceofAdmissionCode");
    }

    public CsvCell getPatientClassification() {
        return super.getCell("PatientClassification");
    }

    public CsvCell getEpisodeNumber() {
        return super.getCell("EpisodeNumber");
    }

    public CsvCell getEpisodeStartSiteCode() {
        return super.getCell("StartSiteCodeofTreatment");
    }

    public CsvCell getEpisodeStartWardCode() {
        return super.getCell("StartWardCode");
    }

    public CsvCell getEpisodeStartDate() {
        return super.getCell("EpisodeStartDate");
    }

    public CsvCell getEpisodeStartTime() {
        return super.getCell("EpisodeStartTime");
    }

    public CsvCell getEpisodeEndSiteCode() {
        return super.getCell("EndSiteCodeofTreatment");
    }

    public CsvCell getEpisodeEndWardCode() {
        return super.getCell("EndWardCode");
    }

    public CsvCell getEpisodeEndDate() {
        return super.getCell("EpisodeEndDate");
    }

    public CsvCell getEpisodeEndTime() {
        return super.getCell("EpisodeEndTime");
    }

    public CsvCell getDischargeDate() {
        return super.getCell("DischargeDateHospitalProviderSpell");
    }

    public CsvCell getDischargeTime() {
        return super.getCell("DischargeTimeHospitalProviderSpell");
    }

    public CsvCell getDischargeDestinationCode() {
        return super.getCell("DischargeDestinationCode");
    }

    public CsvCell getDischargeMethod() {
        return super.getCell("DischargeMethod");
    }

    public CsvCell getBirthWeight()  {
        return super.getCell("BirthWeight");
    }

    public CsvCell getLiveOrStillBirthIndicator()  {
        return super.getCell("LiveOrStillBirthIndicator");
    }

    public CsvCell getTotalPreviousPregnancies()  {
        return super.getCell("TotalPreviousPregnancies");
    }

    public CsvCell getFirstAntenatalAssessmentDate()  {
        return super.getCell("FirstAntenatalAssessmentDate");
    }

    public CsvCell getAntenatalCarePractitioner()  {
        return super.getCell("GeneralMedicalPractitionerAntenatalCare");
    }

    public CsvCell getAntenatalCarePractice()  {
        return super.getCell("GeneralMedicalPracticeAntenatalCare");
    }

    public CsvCell getDeliveryPlaceTypeIntended()  {
        return super.getCell("DeliveryPlaceTypeIntended");
    }

    public CsvCell getDeliveryPlaceChangeReasonCode()  {
        return super.getCell("DeliveryPlaceChangeReasonCode");
    }

    public CsvCell getGestationLengthLabourOnset()  {
        return super.getCell("GestationLengthLabourOnset");
    }

    public CsvCell getDeliveryMethod()  {
        return super.getCell("DeliveryMethod");
    }

    public CsvCell getDeliveryPlaceTypeActual()  {
        return super.getCell("DeliveryPlaceTypeActual");
    }

    // birth records, can be up to 9 based on NumberofBabies count
    public CsvCell getNumberOfBabies()  {
        return super.getCell("NumberofBabies");
    }
    public CsvCell getDeliveryDate()  {
        return super.getCell("DeliveryDate");
    }

    public CsvCell getBirthOrder(int dataNumber) { return super.getCell("BirthOrder"+dataNumber);}
    public CsvCell getDeliveryMethod(int dataNumber)  { return super.getCell("DeliveryMethod"+dataNumber); }
    public CsvCell getBabyNHSNumber(int dataNumber)  { return super.getCell("BabyNHSNumber"+dataNumber); }
    public CsvCell getBabyBirthDate(int dataNumber)  { return super.getCell("BabyBirthDate"+dataNumber); }
    public CsvCell getBirthWeight(int dataNumber)  { return super.getCell("BirthWeight"+dataNumber); }
    public CsvCell getLiveOrStillBirthIndicator(int dataNumber)  { return super.getCell("LiveOrStillBirthIndicator"+dataNumber); }
    public CsvCell getBabyGender(int dataNumber)  { return super.getCell("PersonGenderCurrent"+dataNumber); }
    public CsvCell getMotherNHSNumber()  {
        return super.getCell("MotherNHSNumber");
    }

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

        //note, Barts have confirmed that the Home Delivery and Birth file uses the same specification
        //as the SUS Inpatient file, so the below is copied from there

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
        ret.add(new FixedParserField("PersonMaritalStatus", 1023, 1));
        ret.add(new FixedParserField("MentalHealthActLegalClassificationCodeonAdmission", 1024, 2));
//BIRTH CHARACTERISTICS
        ret.add(new FixedParserField("BirthWeight", 1026, 4));
        ret.add(new FixedParserField("LiveOrStillBirthIndicator", 1030, 1));
//DELIVERY CHARACTERISTICS
        ret.add(new FixedParserField("TotalPreviousPregnancies", 1031, 2));
//HOSPITAL PROVIDER SPELL DETAILS
//Admission Characteristics
        ret.add(new FixedParserField("HospitalProviderSpellNumber", 1033, 12));
        ret.add(new FixedParserField("AdministrativeCategoryCode", 1045, 2));
        ret.add(new FixedParserField("PatientClassification", 1047, 1));
        ret.add(new FixedParserField("AdmissionMethodCode", 1048, 2));
        ret.add(new FixedParserField("SourceofAdmissionCode", 1050, 2));
        ret.add(new FixedParserField("StartDateHospitalProviderSpell", 1052, 8));
        ret.add(new FixedParserField("StartTimeHospitalProviderSpell", 1060, 6));
        ret.add(new FixedParserField("AgeOnAdmission", 1066, 3));
        ret.add(new FixedParserField("AmbulanceIncidentNumber", 1069, 20));
        ret.add(new FixedParserField("OrganisationCodeConveyingAmbulanceTrust", 1089, 12));
//Discharge Characteristics
        ret.add(new FixedParserField("DischargeDestinationCode", 1101, 2));
        ret.add(new FixedParserField("DischargeMethod", 1103, 1));
        ret.add(new FixedParserField("DischargeReadyDateHospitalProviderSpell", 1104, 8));
        ret.add(new FixedParserField("DischargeDateHospitalProviderSpell", 1112, 8));
        ret.add(new FixedParserField("DischargeTimeHospitalProviderSpell", 1120, 6));
        ret.add(new FixedParserField("DischargetoHospitalatHomeServiceIndicator", 1126, 1));
//EPISODE CHARACTERISTICS
        ret.add(new FixedParserField("EpisodeNumber", 1127, 2));
        ret.add(new FixedParserField("LastEpisodeinSpellIndicator", 1129, 1));
        ret.add(new FixedParserField("OperationStatusCode", 1130, 1));
        ret.add(new FixedParserField("NeonatalLevelofCare", 1131, 1));
        ret.add(new FixedParserField("FirstRegularDayOrNightAdmission", 1132, 1));
        ret.add(new FixedParserField("PsychiatricPatientStatus", 1133, 1));
        ret.add(new FixedParserField("EpisodeStartDate", 1134, 8));
        ret.add(new FixedParserField("EpisodeStartTime", 1142, 6));
        ret.add(new FixedParserField("EpisodeEndDate", 1148, 8));
        ret.add(new FixedParserField("EpisodeEndTime", 1156, 6));
        ret.add(new FixedParserField("AgeAtCDSActivityDate", 1162, 3));
        ret.add(new FixedParserField("MultiProfessionalorMultidisciplinaryConsultationIndicationCode", 1165, 1));
        ret.add(new FixedParserField("RehabilitationAssessmentTeamType", 1166, 1));
        ret.add(new FixedParserField("LengthofStayAdjustmentRehabilitation", 1167, 3));
        ret.add(new FixedParserField("LengthOfStayAdjustmentSpecialistPalliativeCare", 1170, 3));
        ret.add(new FixedParserField("OverseasVisitorsStatus1", 1173, 17));
        ret.add(new FixedParserField("OverseasVisitorsClassification", 1173, 1));
        ret.add(new FixedParserField("OverseasVisitorStatusStartDate", 1174, 8));
        ret.add(new FixedParserField("OverseasVisitorStatusEndDate", 1182, 8));
        ret.add(new FixedParserField("OverseasVisitorsStatus2", 1190, 17));
        ret.add(new FixedParserField("OverseasVisitorsStatus3", 1207, 17));
        ret.add(new FixedParserField("OverseasVisitorsStatus4", 1224, 17));
        ret.add(new FixedParserField("OverseasVisitorsStatus5", 1241, 17));
//SERVICE AGREEMENT DETAILS
        ret.add(new FixedParserField("CommissioningSerialNumber", 1258, 6));
        ret.add(new FixedParserField("NHSServiceAgreementLineNumber", 1264, 10));
        ret.add(new FixedParserField("ProviderReferenceNumber", 1274, 17));
        ret.add(new FixedParserField("CommissionerReferenceNumber", 1291, 17));
        ret.add(new FixedParserField("OrganisationCodeCodeofProvider", 1308, 12));
        ret.add(new FixedParserField("OrganisationCodeCodeofCommissioner", 1320, 12));
//PERSON GROUP CONSULTANT
        ret.add(new FixedParserField("ConsultantCode", 1332, 8));
        ret.add(new FixedParserField("CareProfessionalMainSpecialtyCode", 1340, 3));
        ret.add(new FixedParserField("ActivityTreatmentFunctionCode", 1343, 3));
        ret.add(new FixedParserField("LocalSubSpecialtyCode", 1346, 8));
//CLINICAL DIAGNOSIS (ICD) DETAILS
        ret.add(new FixedParserField("DiagnosisSchemeinUse", 1354, 2));
        ret.add(new FixedParserField("PrimaryDiagnosisICD", 1356, 6));
        ret.add(new FixedParserField("PresentOnAdmissionIndicator", 1362, 1));
        ret.add(new FixedParserField("SecondaryDiagnosisICD", 1363, 6));
        ret.add(new FixedParserField("SecondaryPresentOnAdmissionIndicator", 1369, 1));
        ret.add(new FixedParserField("2nd50thSecondaryDiagnosisICD", 1370, 343));
//CLINICAL DIAGNOSIS (READ) DETAILS
        /*ret.add(new FixedParserField("DiagnosisSchemeinUse", 1713, 2));
        ret.add(new FixedParserField("PrimaryDiagnosisRead", 1715, 5));
        ret.add(new FixedParserField("SecondaryDiagnosisRead150", 1720, 250));*/
//CLINICAL TREATMENT (OPCS) DETAILS
        ret.add(new FixedParserField("ProcedureSchemeInUse", 1970, 2));
        ret.add(new FixedParserField("PrimaryProcedureOPCS", 1972, 4));
        ret.add(new FixedParserField("PrimaryProcedureDate", 1976, 8));
        ret.add(new FixedParserField("PrimaryMainOperatingHCPRegistrationIssuerCode", 1984, 2));
        ret.add(new FixedParserField("PrimaryMainOperatingHCPRegistrationEntryIdentifier", 1986, 12));
        ret.add(new FixedParserField("PrimaryResponsibleAnaesthetistRegistrationIssuerCode", 1998, 2));
        ret.add(new FixedParserField("PrimaryResponsibleAnaesthetistRegistrationEntryIdentifier", 2000, 12));
        ret.add(new FixedParserField("SecondaryProcedureOPCS", 2012, 4));
        ret.add(new FixedParserField("SecondaryProcedureDate", 2016, 8));
        ret.add(new FixedParserField("SecondaryMainOperatingHCPRegistrationIssuerCode", 2024, 2));
        ret.add(new FixedParserField("SecondaryMainOperatingHCPRegistrationEntryIdentifier", 2026, 12));
        ret.add(new FixedParserField("SecondaryResponsibleAnaesthetistRegistrationIssuerCode", 2038, 2));
        ret.add(new FixedParserField("SecondaryResponsibleAnaesthetistRegistrationEntryIdentifier", 2040, 12));
        ret.add(new FixedParserField("2nd50thSecondaryProceduresOPCS", 2052, 1960));
//CLINICAL TREATMENT (READ) DETAILS
        /*ret.add(new FixedParserField("ProcedureSchemeInUse", 4012, 2));
        ret.add(new FixedParserField("PrimaryProcedureGroupREAD", 4014, 13));
        ret.add(new FixedParserField("PrimaryProcedureREAD", 4014, 5));
        ret.add(new FixedParserField("PrimaryProcedureDate", 4019, 8));
        ret.add(new FixedParserField("1stSecondaryProcedureGroupREAD", 4027, 13));
        ret.add(new FixedParserField("SecondaryProcedureREAD", 4027, 5));
        ret.add(new FixedParserField("SecondaryProcedureDate", 4032, 8));
        ret.add(new FixedParserField("2nd50thSecondaryProceduresREAD", 4040, 637));*/
//LOCATION GROUP (START OF EPISODE)
        ret.add(new FixedParserField("StartLocationClass", 4677, 2));
        ret.add(new FixedParserField("StartSiteCodeofTreatment", 4679, 12));
        ret.add(new FixedParserField("StartActivityLocationType", 4691, 3));
        ret.add(new FixedParserField("StartIntendedClinicalCareIntensityCode", 4694, 2));
        ret.add(new FixedParserField("StartAgeGroupIntended", 4696, 1));
        ret.add(new FixedParserField("StartSexofPatientsCode", 4697, 1));
        ret.add(new FixedParserField("StartWardNightPeriodAvailabilityCode", 4698, 1));
        ret.add(new FixedParserField("StartWardDayPeriodAvailabilityCode", 4699, 1));
        ret.add(new FixedParserField("StartWardSecurityLevel", 4700, 1));
        ret.add(new FixedParserField("StartWardCode", 4701, 12));
//LOCATION GROUP (WARD STAY)
        /*ret.add(new FixedParserField("LocationDetailsWardStay1", 4713, 64));
        ret.add(new FixedParserField("LocationClass", 4713, 2));
        ret.add(new FixedParserField("SiteCodeofTreatment", 4715, 12));
        ret.add(new FixedParserField("ActivityLocationType", 4727, 3));
        ret.add(new FixedParserField("IntendedClinicalCareIntensityCode", 4730, 2));
        ret.add(new FixedParserField("AgeGroupIntended", 4732, 1));
        ret.add(new FixedParserField("SexofPatientsCode", 4733, 1));
        ret.add(new FixedParserField("WardNightPeriodAvailabilityCode", 4734, 1));
        ret.add(new FixedParserField("WardDayPeriodAvailabilityCode", 4735, 1));
        ret.add(new FixedParserField("StartDate", 4736, 8));
        ret.add(new FixedParserField("StartTime", 4744, 6));
        ret.add(new FixedParserField("EndDate", 4750, 8));
        ret.add(new FixedParserField("EndTime", 4758, 6));
        ret.add(new FixedParserField("WardSecurityLevel", 4764, 1));
        ret.add(new FixedParserField("WardCode", 4765, 12));
        ret.add(new FixedParserField("LocationGroupWardStay2", 4777, 64));
        ret.add(new FixedParserField("LocationClass", 4777, 2));
        ret.add(new FixedParserField("SiteCodeofTreatment", 4779, 12));
        ret.add(new FixedParserField("ActivityLocationType", 4791, 3));
        ret.add(new FixedParserField("IntendedClinicalCareIntensityCode", 4794, 2));
        ret.add(new FixedParserField("AgeGroupIntended", 4796, 1));
        ret.add(new FixedParserField("SexofPatientsCode", 4797, 1));
        ret.add(new FixedParserField("WardNightPeriodAvailabilityCode", 4798, 1));
        ret.add(new FixedParserField("WardDayPeriodAvailabilityCode", 4799, 1));
        ret.add(new FixedParserField("StartDate", 4800, 8));
        ret.add(new FixedParserField("StartTime", 4808, 6));
        ret.add(new FixedParserField("EndDate", 4814, 8));
        ret.add(new FixedParserField("EndTime", 4822, 6));
        ret.add(new FixedParserField("WardSecurityLevel", 4828, 1));
        ret.add(new FixedParserField("WardCode", 4829, 12));
        ret.add(new FixedParserField("LocationGroupWardStay3", 4841, 64));
        ret.add(new FixedParserField("LocationClass", 4841, 2));
        ret.add(new FixedParserField("SiteCodeofTreatment", 4843, 12));
        ret.add(new FixedParserField("ActivityLocationType", 4855, 3));
        ret.add(new FixedParserField("IntendedClinicalCareIntensityCode", 4858, 2));
        ret.add(new FixedParserField("AgeGroupIntended", 4860, 1));
        ret.add(new FixedParserField("SexofPatientsCode", 4861, 1));
        ret.add(new FixedParserField("WardNightPeriodAvailabilityCode", 4862, 1));
        ret.add(new FixedParserField("WardDayPeriodAvailabilityCode", 4863, 1));
        ret.add(new FixedParserField("StartDate", 4864, 8));
        ret.add(new FixedParserField("StartTime", 4872, 6));
        ret.add(new FixedParserField("EndDate", 4878, 8));
        ret.add(new FixedParserField("EndTime", 4886, 6));
        ret.add(new FixedParserField("WardSecurityLevel", 4892, 1));
        ret.add(new FixedParserField("WardCode", 4893, 12));
        ret.add(new FixedParserField("LocationGroupWardStay4", 4905, 64));
        ret.add(new FixedParserField("LocationClass", 4905, 2));
        ret.add(new FixedParserField("SiteCodeofTreatment", 4907, 12));
        ret.add(new FixedParserField("ActivityLocationType", 4919, 3));
        ret.add(new FixedParserField("IntendedClinicalCareIntensityCode", 4922, 2));
        ret.add(new FixedParserField("AgeGroupIntended", 4924, 1));
        ret.add(new FixedParserField("SexofPatientsCode", 4925, 1));
        ret.add(new FixedParserField("WardNightPeriodAvailabilityCode", 4926, 1));
        ret.add(new FixedParserField("WardDayPeriodAvailabilityCode", 4927, 1));
        ret.add(new FixedParserField("StartDate", 4928, 8));
        ret.add(new FixedParserField("StartTime", 4936, 6));
        ret.add(new FixedParserField("EndDate", 4942, 8));
        ret.add(new FixedParserField("EndTime", 4950, 6));
        ret.add(new FixedParserField("WardSecurityLevel", 4956, 1));
        ret.add(new FixedParserField("WardCode", 4957, 12));
        ret.add(new FixedParserField("LocationGroupWardStay5", 4969, 64));
        ret.add(new FixedParserField("LocationClass", 4969, 2));
        ret.add(new FixedParserField("SiteCodeofTreatment", 4971, 12));
        ret.add(new FixedParserField("ActivityLocationType", 4983, 3));
        ret.add(new FixedParserField("IntendedClinicalCareIntensityCode", 4986, 2));
        ret.add(new FixedParserField("AgeGroupIntended", 4988, 1));
        ret.add(new FixedParserField("SexofPatientsCode", 4989, 1));
        ret.add(new FixedParserField("WardNightPeriodAvailabilityCode", 4990, 1));
        ret.add(new FixedParserField("WardDayPeriodAvailabilityCode", 4991, 1));
        ret.add(new FixedParserField("StartDate", 4992, 8));
        ret.add(new FixedParserField("StartTime", 5000, 6));
        ret.add(new FixedParserField("EndDate", 5006, 8));
        ret.add(new FixedParserField("EndTime", 5014, 6));
        ret.add(new FixedParserField("WardSecurityLevel", 5020, 1));
        ret.add(new FixedParserField("WardCode", 5021, 12));
        ret.add(new FixedParserField("LocationGroupWardStay6", 5033, 64));
        ret.add(new FixedParserField("LocationClass", 5033, 2));
        ret.add(new FixedParserField("SiteCodeofTreatment", 5035, 12));
        ret.add(new FixedParserField("ActivityLocationType", 5047, 3));
        ret.add(new FixedParserField("IntendedClinicalCareIntensityCode", 5050, 2));
        ret.add(new FixedParserField("AgeGroupIntended", 5052, 1));
        ret.add(new FixedParserField("SexofPatientsCode", 5053, 1));
        ret.add(new FixedParserField("WardNightPeriodAvailabilityCode", 5054, 1));
        ret.add(new FixedParserField("WardDayPeriodAvailabilityCode", 5055, 1));
        ret.add(new FixedParserField("StartDate", 5056, 8));
        ret.add(new FixedParserField("StartTime", 5064, 6));
        ret.add(new FixedParserField("EndDate", 5070, 8));
        ret.add(new FixedParserField("EndTime", 5078, 6));
        ret.add(new FixedParserField("WardSecurityLevel", 5084, 1));
        ret.add(new FixedParserField("WardCode", 5085, 12));
        ret.add(new FixedParserField("LocationGroupWardStay7", 5097, 64));
        ret.add(new FixedParserField("LocationClass", 5097, 2));
        ret.add(new FixedParserField("SiteCodeofTreatment", 5099, 12));
        ret.add(new FixedParserField("ActivityLocationType", 5111, 3));
        ret.add(new FixedParserField("IntendedClinicalCareIntensityCode", 5114, 2));
        ret.add(new FixedParserField("AgeGroupIntended", 5116, 1));
        ret.add(new FixedParserField("SexofPatientsCode", 5117, 1));
        ret.add(new FixedParserField("WardNightPeriodAvailabilityCode", 5118, 1));
        ret.add(new FixedParserField("WardDayPeriodAvailabilityCode", 5119, 1));
        ret.add(new FixedParserField("StartDate", 5120, 8));
        ret.add(new FixedParserField("StartTime", 5128, 6));
        ret.add(new FixedParserField("EndDate", 5134, 8));
        ret.add(new FixedParserField("EndTime", 5142, 6));
        ret.add(new FixedParserField("WardSecurityLevel", 5148, 1));
        ret.add(new FixedParserField("WardCode", 5149, 12));
        ret.add(new FixedParserField("LocationGroupWardStay8", 5161, 64));
        ret.add(new FixedParserField("LocationClass", 5161, 2));
        ret.add(new FixedParserField("SiteCodeofTreatment", 5163, 12));
        ret.add(new FixedParserField("ActivityLocationType", 5175, 3));
        ret.add(new FixedParserField("IntendedClinicalCareIntensityCode", 5178, 2));
        ret.add(new FixedParserField("AgeGroupIntended", 5180, 1));
        ret.add(new FixedParserField("SexofPatientsCode", 5181, 1));
        ret.add(new FixedParserField("WardNightPeriodAvailabilityCode", 5182, 1));
        ret.add(new FixedParserField("WardDayPeriodAvailabilityCode", 5183, 1));
        ret.add(new FixedParserField("StartDate", 5184, 8));
        ret.add(new FixedParserField("StartTime", 5192, 6));
        ret.add(new FixedParserField("EndDate", 5198, 8));
        ret.add(new FixedParserField("EndTime", 5206, 6));
        ret.add(new FixedParserField("WardSecurityLevel", 5212, 1));
        ret.add(new FixedParserField("WardCode", 5213, 12));
        ret.add(new FixedParserField("LocationGroupWardStay9", 5225, 64));
        ret.add(new FixedParserField("LocationClass", 5225, 2));
        ret.add(new FixedParserField("SiteCodeofTreatment", 5227, 12));
        ret.add(new FixedParserField("ActivityLocationType", 5239, 3));
        ret.add(new FixedParserField("IntendedClinicalCareIntensityCode", 5242, 2));
        ret.add(new FixedParserField("AgeGroupIntended", 5244, 1));
        ret.add(new FixedParserField("SexofPatientsCode", 5245, 1));
        ret.add(new FixedParserField("WardNightPeriodAvailabilityCode", 5246, 1));
        ret.add(new FixedParserField("WardDayPeriodAvailabilityCode", 5247, 1));
        ret.add(new FixedParserField("StartDate", 5248, 8));
        ret.add(new FixedParserField("StartTime", 5256, 6));
        ret.add(new FixedParserField("EndDate", 5262, 8));
        ret.add(new FixedParserField("EndTime", 5270, 6));
        ret.add(new FixedParserField("WardSecurityLevel", 5276, 1));
        ret.add(new FixedParserField("WardCode", 5277, 12));*/
//LOCATION GROUP (END OF EPISODE)
        ret.add(new FixedParserField("EndLocationClass", 5289, 2));
        ret.add(new FixedParserField("EndSiteCodeofTreatment", 5291, 12));
        ret.add(new FixedParserField("EndActivityLocationType", 5303, 3));
        ret.add(new FixedParserField("EndIntendedClinicalCareIntensityCode", 5306, 2));
        ret.add(new FixedParserField("EndAgeGroupIntended", 5308, 1));
        ret.add(new FixedParserField("EndSexofPatientsCode", 5309, 1));
        ret.add(new FixedParserField("EndWardNightPeriodAvailabilityCode", 5310, 1));
        ret.add(new FixedParserField("EndWardDayPeriodAvailabilityCode", 5311, 1));
        ret.add(new FixedParserField("EndWardSecurityLevel", 5312, 1));
        ret.add(new FixedParserField("EndWardCode", 5313, 12));
//G.P. DETAILS
        ret.add(new FixedParserField("GeneralMedicalPractitionerRegistered", 5325, 8));
        ret.add(new FixedParserField("GPPracticeRegistered", 5333, 12));
//REFERRAL DETAILS
        ret.add(new FixedParserField("ReferrerCode", 5345, 8));
        ret.add(new FixedParserField("ReferringOrganisationCode", 5353, 12));
        ret.add(new FixedParserField("DirectAccessReferralIndicator", 5365, 1));
//EAL ENTRY APC REFERENCE
        ret.add(new FixedParserField("DurationOfElectiveWait", 5366, 4));
        ret.add(new FixedParserField("IntendedManagement", 5370, 1));
        ret.add(new FixedParserField("DecidedToAdmitDate", 5371, 8));
        ret.add(new FixedParserField("EarliestReasonableOfferDate", 5379, 8));
//PREGNANCY DETAILS
        ret.add(new FixedParserField("NumberofBabies", 5387, 1));
//ANTENATAL CARE
        ret.add(new FixedParserField("FirstAntenatalAssessmentDate", 5388, 8));
        ret.add(new FixedParserField("GeneralMedicalPractitionerAntenatalCare", 5396, 8));
        ret.add(new FixedParserField("GeneralMedicalPracticeAntenatalCare", 5404, 12));
        ret.add(new FixedParserField("LocationClass", 5416, 2));
        ret.add(new FixedParserField("ActivityLocationType", 5418, 3));
        ret.add(new FixedParserField("DeliveryPlaceTypeIntended", 5421, 1));
        ret.add(new FixedParserField("DeliveryPlaceChangeReasonCode", 5422, 1));
//LABOUR DELIVERY
        ret.add(new FixedParserField("AnaestheticDuringLabourOrDelivery", 5423, 1));
        ret.add(new FixedParserField("AnaestheticGivenPostLabourOrDelivery", 5424, 1));
        ret.add(new FixedParserField("GestationLengthLabourOnset", 5425, 2));
        ret.add(new FixedParserField("LabourOrDeliveryOnsetMethodCode", 5427, 1));
        ret.add(new FixedParserField("DeliveryDate", 5428, 8));
//BIRTH DETAILS
        /*ret.add(new FixedParserField("BabyDetails1", 5436, 64));
        ret.add(new FixedParserField("BirthOrder", 5436, 1));*/
        ret.add(new FixedParserField("DeliveryMethod", 5437, 1));
        /*ret.add(new FixedParserField("GestationLengthAssessment", 5438, 2));
        ret.add(new FixedParserField("ResuscitationMethod", 5440, 1));
        ret.add(new FixedParserField("StatusofPersonConductingDelivery", 5441, 1));
        ret.add(new FixedParserField("LocationClass", 5442, 2));*/
        ret.add(new FixedParserField("DeliveryPlaceTypeActual", 5444, 1));
        /* ret.add(new FixedParserField("ActivityLocationType", 5445, 3));
        ret.add(new FixedParserField("LocalPatientID", 5448, 10));
        ret.add(new FixedParserField("OrganisationCodeLocalPatientID", 5458, 12));
        ret.add(new FixedParserField("NHSNumber", 5470, 10));
        ret.add(new FixedParserField("NHSNumberStatusIndicator", 5480, 2));
        ret.add(new FixedParserField("WithheldFlag", 5482, 1));
        ret.add(new FixedParserField("WithheldIdentityReason", 5483, 2));
        ret.add(new FixedParserField("BabyBirthDate", 5485, 8));
        ret.add(new FixedParserField("BirthWeight", 5493, 4));
        ret.add(new FixedParserField("LiveOrStillBirth", 5497, 1));
        ret.add(new FixedParserField("PersonGenderCurrent", 5498, 1));
        ret.add(new FixedParserField("OverseasVisitorStatusClassificationAtCDSActivityDate", 5499, 1));
        ret.add(new FixedParserField("BabyDetails2", 5500, 64));
        ret.add(new FixedParserField("BirthOrder", 5500, 1));
        ret.add(new FixedParserField("DeliveryMethod", 5501, 1));
        ret.add(new FixedParserField("GestationLengthAssessment", 5502, 2));
        ret.add(new FixedParserField("ResuscitationMethod", 5504, 1));
        ret.add(new FixedParserField("StatusofPersonConductingDelivery", 5505, 1));
        ret.add(new FixedParserField("LocationClass", 5506, 2));
        ret.add(new FixedParserField("DeliveryPlaceTypeActual", 5508, 1));
        ret.add(new FixedParserField("ActivityLocationType", 5509, 3));
        ret.add(new FixedParserField("LocalPatientID", 5512, 10));
        ret.add(new FixedParserField("OrganisationCodeLocalPatientID", 5522, 12));
        ret.add(new FixedParserField("NHSNumber", 5534, 10));
        ret.add(new FixedParserField("NHSNumberStatusIndicator", 5544, 2));
        ret.add(new FixedParserField("WithheldFlag", 5546, 1));
        ret.add(new FixedParserField("WithheldIdentityReason", 5547, 2));
        ret.add(new FixedParserField("BabyBirthDate", 5549, 8));
        ret.add(new FixedParserField("BirthWeight", 5557, 4));
        ret.add(new FixedParserField("LiveOrStillBirth", 5561, 1));
        ret.add(new FixedParserField("PersonGenderCurrent", 5562, 1));
        ret.add(new FixedParserField("OverseasVisitorStatusClassificationAtCDSActivityDate", 5563, 1));
        ret.add(new FixedParserField("BabyDetails3", 5564, 64));
        ret.add(new FixedParserField("BirthOrder", 5564, 1));
        ret.add(new FixedParserField("DeliveryMethod", 5565, 1));
        ret.add(new FixedParserField("GestationLengthAssessment", 5566, 2));
        ret.add(new FixedParserField("ResuscitationMethod", 5568, 1));
        ret.add(new FixedParserField("StatusofPersonConductingDelivery", 5569, 1));
        ret.add(new FixedParserField("LocationClass", 5570, 2));
        ret.add(new FixedParserField("DeliveryPlaceTypeActual", 5572, 1));
        ret.add(new FixedParserField("ActivityLocationType", 5573, 3));
        ret.add(new FixedParserField("LocalPatientID", 5576, 10));
        ret.add(new FixedParserField("OrganisationCodeLocalPatientID", 5586, 12));
        ret.add(new FixedParserField("NHSNumber", 5598, 10));
        ret.add(new FixedParserField("NHSNumberStatusIndicator", 5608, 2));
        ret.add(new FixedParserField("WithheldFlag", 5610, 1));
        ret.add(new FixedParserField("WithheldIdentityReason", 5611, 2));
        ret.add(new FixedParserField("BabyBirthDate", 5613, 8));
        ret.add(new FixedParserField("BirthWeight", 5621, 4));
        ret.add(new FixedParserField("LiveOrStillBirth", 5625, 1));
        ret.add(new FixedParserField("PersonGenderCurrent", 5626, 1));
        ret.add(new FixedParserField("OverseasVisitorStatusClassificationAtCDSActivityDate", 5627, 1));
        ret.add(new FixedParserField("BabyDetails4", 5628, 64));
        ret.add(new FixedParserField("BirthOrder", 5628, 1));
        ret.add(new FixedParserField("DeliveryMethod", 5629, 1));
        ret.add(new FixedParserField("GestationLengthAssessment", 5630, 2));
        ret.add(new FixedParserField("ResuscitationMethod", 5632, 1));
        ret.add(new FixedParserField("StatusofPersonConductingDelivery", 5633, 1));
        ret.add(new FixedParserField("LocationClass", 5634, 2));
        ret.add(new FixedParserField("DeliveryPlaceTypeActual", 5636, 1));
        ret.add(new FixedParserField("ActivityLocationType", 5637, 3));
        ret.add(new FixedParserField("LocalPatientID", 5640, 10));
        ret.add(new FixedParserField("OrganisationCodeLocalPatientID", 5650, 12));
        ret.add(new FixedParserField("NHSNumber", 5662, 10));
        ret.add(new FixedParserField("NHSNumberStatusIndicator", 5672, 2));
        ret.add(new FixedParserField("WithheldFlag", 5674, 1));
        ret.add(new FixedParserField("WithheldIdentityReason", 5675, 2));
        ret.add(new FixedParserField("BabyBirthDate", 5677, 8));
        ret.add(new FixedParserField("BirthWeight", 5685, 4));
        ret.add(new FixedParserField("LiveOrStillBirth", 5689, 1));
        ret.add(new FixedParserField("PersonGenderCurrent", 5690, 1));
        ret.add(new FixedParserField("OverseasVisitorStatusClassificationAtCDSActivityDate", 5691, 1));
        ret.add(new FixedParserField("BabyDetails5", 5692, 64));
        ret.add(new FixedParserField("BirthOrder", 5692, 1));
        ret.add(new FixedParserField("DeliveryMethod", 5693, 1));
        ret.add(new FixedParserField("GestationLengthAssessment", 5694, 2));
        ret.add(new FixedParserField("ResuscitationMethod", 5696, 1));
        ret.add(new FixedParserField("StatusofPersonConductingDelivery", 5697, 1));
        ret.add(new FixedParserField("LocationClass", 5698, 2));
        ret.add(new FixedParserField("DeliveryPlaceTypeActual", 5700, 1));
        ret.add(new FixedParserField("ActivityLocationType", 5701, 3));
        ret.add(new FixedParserField("LocalPatientID", 5704, 10));
        ret.add(new FixedParserField("OrganisationCodeLocalPatientID", 5714, 12));
        ret.add(new FixedParserField("NHSNumber", 5726, 10));
        ret.add(new FixedParserField("NHSNumberStatusIndicator", 5736, 2));
        ret.add(new FixedParserField("WithheldFlag", 5738, 1));
        ret.add(new FixedParserField("WithheldIdentityReason", 5739, 2));
        ret.add(new FixedParserField("BabyBirthDate", 5741, 8));
        ret.add(new FixedParserField("BirthWeight", 5749, 4));
        ret.add(new FixedParserField("LiveOrStillBirth", 5753, 1));
        ret.add(new FixedParserField("PersonGenderCurrent", 5754, 1));
        ret.add(new FixedParserField("OverseasVisitorStatusClassificationAtCDSActivityDate", 5755, 1));
        ret.add(new FixedParserField("BabyDetails6", 5756, 64));
        ret.add(new FixedParserField("BirthOrder", 5756, 1));
        ret.add(new FixedParserField("DeliveryMethod", 5757, 1));
        ret.add(new FixedParserField("GestationLengthAssessment", 5758, 2));
        ret.add(new FixedParserField("ResuscitationMethod", 5760, 1));
        ret.add(new FixedParserField("StatusofPersonConductingDelivery", 5761, 1));
        ret.add(new FixedParserField("LocationClass", 5762, 2));
        ret.add(new FixedParserField("DeliveryPlaceTypeActual", 5764, 1));
        ret.add(new FixedParserField("ActivityLocationType", 5765, 3));
        ret.add(new FixedParserField("LocalPatientID", 5768, 10));
        ret.add(new FixedParserField("OrganisationCodeLocalPatientID", 5778, 12));
        ret.add(new FixedParserField("NHSNumber", 5790, 10));
        ret.add(new FixedParserField("NHSNumberStatusIndicator", 5800, 2));
        ret.add(new FixedParserField("WithheldFlag", 5802, 1));
        ret.add(new FixedParserField("WithheldIdentityReason", 5803, 2));
        ret.add(new FixedParserField("BabyBirthDate", 5805, 8));
        ret.add(new FixedParserField("BirthWeight", 5813, 4));
        ret.add(new FixedParserField("LiveOrStillBirth", 5817, 1));
        ret.add(new FixedParserField("PersonGenderCurrent", 5818, 1));
        ret.add(new FixedParserField("OverseasVisitorStatusClassificationAtCDSActivityDate", 5819, 1));
        ret.add(new FixedParserField("BabyDetails7", 5820, 64));
        ret.add(new FixedParserField("BirthOrder", 5820, 1));
        ret.add(new FixedParserField("DeliveryMethod", 5821, 1));
        ret.add(new FixedParserField("GestationLengthAssessment", 5822, 2));
        ret.add(new FixedParserField("ResuscitationMethod", 5824, 1));
        ret.add(new FixedParserField("StatusofPersonConductingDelivery", 5825, 1));
        ret.add(new FixedParserField("LocationClass", 5826, 2));
        ret.add(new FixedParserField("DeliveryPlaceTypeActual", 5828, 1));
        ret.add(new FixedParserField("ActivityLocationType", 5829, 3));
        ret.add(new FixedParserField("LocalPatientID", 5832, 10));
        ret.add(new FixedParserField("OrganisationCodeLocalPatientID", 5842, 12));
        ret.add(new FixedParserField("NHSNumber", 5854, 10));
        ret.add(new FixedParserField("NHSNumberStatusIndicator", 5864, 2));
        ret.add(new FixedParserField("WithheldFlag", 5866, 1));
        ret.add(new FixedParserField("WithheldIdentityReason", 5867, 2));
        ret.add(new FixedParserField("BabyBirthDate", 5869, 8));
        ret.add(new FixedParserField("BirthWeight", 5877, 4));
        ret.add(new FixedParserField("LiveOrStillBirth", 5881, 1));
        ret.add(new FixedParserField("PersonGenderCurrent", 5882, 1));
        ret.add(new FixedParserField("OverseasVisitorStatusClassificationAtCDSActivityDate", 5883, 1));
        ret.add(new FixedParserField("BabyDetails8", 5884, 64));
        ret.add(new FixedParserField("BirthOrder", 5884, 1));
        ret.add(new FixedParserField("DeliveryMethod", 5885, 1));
        ret.add(new FixedParserField("GestationLengthAssessment", 5886, 2));
        ret.add(new FixedParserField("ResuscitationMethod", 5888, 1));
        ret.add(new FixedParserField("StatusofPersonConductingDelivery", 5889, 1));
        ret.add(new FixedParserField("LocationClass", 5890, 2));
        ret.add(new FixedParserField("DeliveryPlaceTypeActual", 5892, 1));
        ret.add(new FixedParserField("ActivityLocationType", 5893, 3));
        ret.add(new FixedParserField("LocalPatientID", 5896, 10));
        ret.add(new FixedParserField("OrganisationCodeLocalPatientID", 5906, 12));
        ret.add(new FixedParserField("NHSNumber", 5918, 10));
        ret.add(new FixedParserField("NHSNumberStatusIndicator", 5928, 2));
        ret.add(new FixedParserField("WithheldFlag", 5930, 1));
        ret.add(new FixedParserField("WithheldIdentityReason", 5931, 2));
        ret.add(new FixedParserField("BabyBirthDate", 5933, 8));
        ret.add(new FixedParserField("BirthWeight", 5941, 4));
        ret.add(new FixedParserField("LiveOrStillBirth", 5945, 1));
        ret.add(new FixedParserField("PersonGenderCurrent", 5946, 1));
        ret.add(new FixedParserField("OverseasVisitorStatusClassificationAtCDSActivityDate", 5947, 1));
        ret.add(new FixedParserField("BabyDetails9", 5948, 64));
        ret.add(new FixedParserField("BirthOrder", 5948, 1));
        ret.add(new FixedParserField("DeliveryMethod", 5949, 1));
        ret.add(new FixedParserField("GestationLengthAssessment", 5950, 2));
        ret.add(new FixedParserField("ResuscitationMethod", 5952, 1));
        ret.add(new FixedParserField("StatusofPersonConductingDelivery", 5953, 1));
        ret.add(new FixedParserField("LocationClass", 5954, 2));
        ret.add(new FixedParserField("DeliveryPlaceTypeActual", 5956, 1));
        ret.add(new FixedParserField("ActivityLocationType", 5957, 3));
        ret.add(new FixedParserField("LocalPatientID", 5960, 10));
        ret.add(new FixedParserField("OrganisationCodeLocalPatientID", 5970, 12));
        ret.add(new FixedParserField("NHSNumber", 5982, 10));
        ret.add(new FixedParserField("NHSNumberStatusIndicator", 5992, 2));
        ret.add(new FixedParserField("WithheldFlag", 5994, 1));
        ret.add(new FixedParserField("WithheldIdentityReason", 5995, 2));
        ret.add(new FixedParserField("BabyBirthDate", 5997, 8));
        ret.add(new FixedParserField("BirthWeight", 6005, 4));
        ret.add(new FixedParserField("LiveOrStillBirth", 6009, 1));
        ret.add(new FixedParserField("PersonGenderCurrent", 6010, 1));
        ret.add(new FixedParserField("OverseasVisitorStatusClassificationAtCDSActivityDate", 6011, 1));*/
//MOTHER DETAILS
        ret.add(new FixedParserField("MotherLocalPatientID", 6012, 10));
        ret.add(new FixedParserField("MotherOrganisationCodeLocalPatientID", 6022, 12));
        ret.add(new FixedParserField("MotherNHSNumberStatusIndicator", 6034, 2));
        ret.add(new FixedParserField("MotherNHSNumber", 6036, 10));
        ret.add(new FixedParserField("MotherWithheldFlag", 6046, 1));
        ret.add(new FixedParserField("MotherWithheldIdentityReason", 6047, 2));
        ret.add(new FixedParserField("MotherBirthDate", 6049, 8));
        ret.add(new FixedParserField("MotherOverseasVisitorStatusAtCDSActivityDate", 6057, 1));
//MOTHER ADDRESS
        /*ret.add(new FixedParserField("PatientAddressType", 6058, 2));
        ret.add(new FixedParserField("PatientUnstructuredAddress", 6060, 175));
        ret.add(new FixedParserField("PatientStructured1", 6235, 35));
        ret.add(new FixedParserField("PatientStructured2", 6270, 35));
        ret.add(new FixedParserField("PatientStructured3", 6305, 35));
        ret.add(new FixedParserField("PatientStructured4", 6340, 35));
        ret.add(new FixedParserField("PatientStructured5", 6375, 35));
        ret.add(new FixedParserField("Postcode", 6410, 8));*/
//MOTHER ORGANISATION RESIDENCE
        /*ret.add(new FixedParserField("OrganisationCodeResidenceResponsibility", 6418, 12));*/


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
        ret.add(new FixedParserField("MaritalStatus",    1023, 1));

        ret.add(new FixedParserField("AdmissionDate",    1052, 8));
        ret.add(new FixedParserField("AdmissionTime",    1060, 6));
        ret.add(new FixedParserField("DischargeDate",    1112, 8));
        ret.add(new FixedParserField("DischargeTime",    1120, 6));

        ret.add(new FixedParserField("ConsultantCode",    1332, 8));

        ret.add(new FixedParserField("ICDPrimaryDiagnosis",    1356, 6));
        ret.add(new FixedParserField("ICDSecondaryDiagnosisList",    1363, 350));

        ret.add(new FixedParserField("OPCSPrimaryProcedureCode",    1972, 4));
        ret.add(new FixedParserField("OPCSPrimaryProcedureDate",    1976, 8));
        ret.add(new FixedParserField("OPCSecondaryProcedureList",    2012, 2000));

        ret.add(new FixedParserField("GP",    5325, 8));
        ret.add(new FixedParserField("GPPractice",    5333, 12));

        return ret;
    }*/
}