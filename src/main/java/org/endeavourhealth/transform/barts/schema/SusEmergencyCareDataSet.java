package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SusEmergencyCareDataSet extends AbstractCsvParser {
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
        return new String[] {
                "CDSTypeCode",
                "CDSProtocolType",
                "BulkReplacementGroup",
                "CDSUniqueIdentifier",
                "CDSUpdateType",
                "CDSApplicableDate",
                "CDSApplicableTime",
                "CDSExtractDate",
                "CDSExtractTime",
                "CDSReportStartDate",
                "CDSReportEndDate",
                "CDSActivityDate",
                "OrganisationCodeSender",
                "OrganisationCodePrimeRecipient",
                "OrganisationCodeCopyRecipient1",
                "OrganisationCodeCopyRecipient2",
                "OrganisationCodeCopyRecipient3",
                "OrganisationCodeCopyRecipient4",
                "OrganisationCodeCopyRecipient5",
                "OrganisationCodeCopyRecipient6",
                "OrganisationCodeCopyRecipient7",
                "CommissioningSerialNumber",
                "NHSServiceAgreementLineNumber",
                "ProviderReferenceNumber",
                "CommissioningReferenceNumber",
                "OrganisationCodeCodeofCommissioner",
                "OrganisationCodeCodeofProvider",
                "UniqueBookingReferencenumber",
                "PatientPathwayIdentifier",
                "OrganisationCodePatientIdentifierIssuer",
                "ReferraltoTreatmentPeriodStatus",
                "WaitingTimeMeasurementType",
                "ReferraltoTreatmentPeriodStartDate",
                "ReferraltoTreatmentPeriodEndDate",
                "WithheldReason",
                "NHSNumber",
                "NHSNumberStatus",
                "LocalPatientID",
                "OrganisationCodeLocalPatientID",
                "PersonFullName",
                "PersonTitle",
                "PersonGivenName",
                "PersonFamilyName",
                "PersonNameSuffix",
                "PersonInitials",
                "DateofBirth",
                "PatientUnstructuredAddress",
                "PatientStructuredAddressline1",
                "PatientStructuredAddressline2",
                "PatientStructuredAddressline3",
                "PatientStructuredAddressline4",
                "PatientStructuredAddressline5",
                "Postcode",
                "OrganisationCodeResidenceResponsibility",
                "PersonCurrentGender",
                "EthnicCategory",
                "AccommodationStatusSnomed",
                "PreferredSpokenLanguage",
                "AccessibleInformationProfessionalRequiredCodeSNOMEDCT",
                "InterpreterLanguageSNOMEDCT",
                "OverseasVisitorChargingCategoryatCDSActivityDate",
                "StartDateMHALegalStatusClassificationAssignmentPeriod1",
                "StartTimeMHALegalStatusClassificationAssignmentPeriod1",
                "ExpiryDateMHALegalStatusClassification1",
                "ExpiryTimeMHALegalStatusClassification1",
                "MHALegalStatusClassificationCode1",
                "StartDateMHALegalStatusClassificationAssignmentPeriod2",
                "StartTimeMHALegalStatusClassificationAssignmentPeriod2",
                "ExpiryDateMHALegalStatusClassification2",
                "ExpiryTimeMHALegalStatusClassification2",
                "MHALegalStatusClassificationCode2",
                "StartDateMHALegalStatusClassificationAssignmentPeriod3",
                "StartTimeMHALegalStatusClassificationAssignmentPeriod3",
                "ExpiryDateMHALegalStatusClassification3",
                "ExpiryTimeMHALegalStatusClassification3",
                "MHALegalStatusClassificationCode3",
                "StartDateMHALegalStatusClassificationAssignmentPeriod4",
                "StartTimeMHALegalStatusClassificationAssignmentPeriod4",
                "ExpiryDateMHALegalStatusClassification4",
                "ExpiryTimeMHALegalStatusClassification4",
                "MHALegalStatusClassificationCode4",
                "StartDateMHALegalStatusClassificationAssignmentPeriod5",
                "StartTimeMHALegalStatusClassificationAssignmentPeriod5",
                "ExpiryDateMHALegalStatusClassification5",
                "ExpiryTimeMHALegalStatusClassification5",
                "MHALegalStatusClassificationCode5",
                "StartDateMHALegalStatusClassificationAssignmentPeriod6",
                "StartTimeMHALegalStatusClassificationAssignmentPeriod6",
                "ExpiryDateMHALegalStatusClassification6",
                "ExpiryTimeMHALegalStatusClassification6",
                "MHALegalStatusClassificationCode6",
                "StartDateMHALegalStatusClassificationAssignmentPeriod7",
                "StartTimeMHALegalStatusClassificationAssignmentPeriod7",
                "ExpiryDateMHALegalStatusClassification7",
                "ExpiryTimeMHALegalStatusClassification7",
                "MHALegalStatusClassificationCode7",
                "StartDateMHALegalStatusClassificationAssignmentPeriod8",
                "StartTimeMHALegalStatusClassificationAssignmentPeriod8",
                "ExpiryDateMHALegalStatusClassification8",
                "ExpiryTimeMHALegalStatusClassification8",
                "MHALegalStatusClassificationCode8",
                "StartDateMHALegalStatusClassificationAssignmentPeriod9",
                "StartTimeMHALegalStatusClassificationAssignmentPeriod9",
                "ExpiryDateMHALegalStatusClassification9",
                "ExpiryTimeMHALegalStatusClassification9",
                "MHALegalStatusClassificationCode9",
                "StartDateMHALegalStatusClassificationAssignmentPeriod10",
                "StartTimeMHALegalStatusClassificationAssignmentPeriod10",
                "ExpiryDateMHALegalStatusClassification10",
                "ExpiryTimeMHALegalStatusClassification10",
                "MHALegalStatusClassificationCode10",
                "GeneralMedicalPractionerSpecified",
                "GeneralMedicalPracticeCodePatientRegistration",
                "OrganisationSiteIdentifierofTreatment",
                "EmergencyCareDepartmentType",
                "AmbulanceIncidentNumber",
                "OrganisationCodeConveyingAmbulanceTrust",
                "EmergencyCareAttendanceIdentifier",
                "EmergencyCareArrivalMode",
                "EmergencyCareAttendanceCategory",
                "EmergencyCareAttendanceSource",
                "EmergencyCareOrganisationCodeAttendanceSource",
                "EmergencyCareArrivalDate",
                "EmergencyCareArrivalTime",
                "EmergencyCareAgeatCDSActivityDate",
                "EmergencyCareInitialAssessmentDate",
                "EmergencyCareInitialAssessmentTime",
                "EmergencyCareAcuitySnomed",
                "EmergencyCareChiefComplaint",
                "EmergencyCareDateSeenforTreatment",
                "EmergencyCareTimeSeenforTreatment",
                "InjuryCharacteristicsInjuryDate",
                "InjuryCharacteristicsInjuryTime",
                "InjuryCharacteristicsPlaceofInjury",
                "InjuryCharacteristicsPlaceofInjuryLatitude",
                "InjuryCharacteristicsPlaceofInjuryLongitude",
                "InjuryCharacteristicsInjuryIntent",
                "InjuryCharacteristicsActivityStatus",
                "InjuryCharacteristicsActivityType",
                "InjuryCharacteristicsActivityMechanism",
                "InjuryCharacteristicsActivityAlcoholandDrugInvolvement1",
                "InjuryCharacteristicsActivityAlcoholandDrugInvolvement2",
                "InjuryCharacteristicsActivityAlcoholandDrugInvolvement3",
                "InjuryCharacteristicsActivityAlcoholandDrugInvolvement4",
                "InjuryCharacteristicsActivityAlcoholandDrugInvolvement5",
                "InjuryCharacteristicsActivityAlcoholandDrugInvolvement6",
                "InjuryCharacteristicsActivityAlcoholandDrugInvolvement7",
                "InjuryCharacteristicsActivityAlcoholandDrugInvolvement8",
                "InjuryCharacteristicsActivityAlcoholandDrugInvolvement9",
                "InjuryCharacteristicsActivityAlcoholandDrugInvolvement10",
                "InjuryCharacteristicsComorbidities1",
                "InjuryCharacteristicsComorbidities2",
                "InjuryCharacteristicsComorbidities3",
                "InjuryCharacteristicsComorbidities4",
                "InjuryCharacteristicsComorbidities5",
                "InjuryCharacteristicsComorbidities6",
                "InjuryCharacteristicsComorbidities7",
                "InjuryCharacteristicsComorbidities8",
                "InjuryCharacteristicsComorbidities9",
                "InjuryCharacteristicsComorbidities10",
                "CareProfessionalRegistrationIssuerCode1",
                "CareProfessionalRegistrationEntryIdentifier1",
                "CareProfessionalTierEmergencyCare1",
                "CareProfessionalDischargeResponsibilityIndicator1",
                "CareProfessionalRegistrationIssuerCode2",
                "CareProfessionalRegistrationEntryIdentifier2",
                "CareProfessionalTierEmergencyCare2",
                "CareProfessionalDischargeResponsibilityIndicator2",
                "CareProfessionalRegistrationIssuerCode3",
                "CareProfessionalRegistrationEntryIdentifier3",
                "CareProfessionalTierEmergencyCare3",
                "CareProfessionalDischargeResponsibilityIndicator3",
                "CareProfessionalRegistrationIssuerCode4",
                "CareProfessionalRegistrationEntryIdentifier4",
                "CareProfessionalTierEmergencyCare4",
                "CareProfessionalDischargeResponsibilityIndicator4",
                "CareProfessionalRegistrationIssuerCode5",
                "CareProfessionalRegistrationEntryIdentifier5",
                "CareProfessionalTierEmergencyCare5",
                "CareProfessionalDischargeResponsibilityIndicator5",
                "CareProfessionalRegistrationIssuerCode6",
                "CareProfessionalRegistrationEntryIdentifier6",
                "CareProfessionalTierEmergencyCare6",
                "CareProfessionalDischargeResponsibilityIndicator6",
                "CareProfessionalRegistrationIssuerCode7",
                "CareProfessionalRegistrationEntryIdentifier7",
                "CareProfessionalTierEmergencyCare7",
                "CareProfessionalDischargeResponsibilityIndicator7",
                "CareProfessionalRegistrationIssuerCode8",
                "CareProfessionalRegistrationEntryIdentifier8",
                "CareProfessionalTierEmergencyCare8",
                "CareProfessionalDischargeResponsibilityIndicator8",
                "CareProfessionalRegistrationIssuerCode9",
                "CareProfessionalRegistrationEntryIdentifier9",
                "CareProfessionalTierEmergencyCare9",
                "CareProfessionalDischargeResponsibilityIndicator9",
                "CareProfessionalRegistrationIssuerCode10",
                "CareProfessionalRegistrationEntryIdentifier10",
                "CareProfessionalTierEmergencyCare10",
                "CareProfessionalDischargeResponsibilityIndicator10",
                "EmergencyCareDiagnosis1",
                "CodedClinicalEntrySequenceNumber1",
                "EmergencyCareDiagnosisQualifier1",
                "EmergencyCareDiagnosis2",
                "CodedClinicalEntrySequenceNumber2",
                "EmergencyCareDiagnosisQualifier2",
                "EmergencyCareDiagnosis3",
                "CodedClinicalEntrySequenceNumber3",
                "EmergencyCareDiagnosisQualifier3",
                "EmergencyCareDiagnosis4",
                "CodedClinicalEntrySequenceNumber4",
                "EmergencyCareDiagnosisQualifier4",
                "EmergencyCareDiagnosis5",
                "CodedClinicalEntrySequenceNumber5",
                "EmergencyCareDiagnosisQualifier5",
                "EmergencyCareDiagnosis6",
                "CodedClinicalEntrySequenceNumber6",
                "EmergencyCareDiagnosisQualifier6",
                "EmergencyCareDiagnosis7",
                "CodedClinicalEntrySequenceNumber7",
                "EmergencyCareDiagnosisQualifier7",
                "EmergencyCareDiagnosis8",
                "CodedClinicalEntrySequenceNumber8",
                "EmergencyCareDiagnosisQualifier8",
                "EmergencyCareDiagnosis9",
                "CodedClinicalEntrySequenceNumber9",
                "EmergencyCareDiagnosisQualifier9",
                "EmergencyCareDiagnosis10",
                "CodedClinicalEntrySequenceNumber10",
                "EmergencyCareDiagnosisQualifier10",
                "EmergencyCareDiagnosis11",
                "CodedClinicalEntrySequenceNumber11",
                "EmergencyCareDiagnosisQualifier11",
                "EmergencyCareDiagnosis12",
                "CodedClinicalEntrySequenceNumber12",
                "EmergencyCareDiagnosisQualifier12",
                "EmergencyCareDiagnosis13",
                "CodedClinicalEntrySequenceNumber13",
                "EmergencyCareDiagnosisQualifier13",
                "EmergencyCareDiagnosis14",
                "CodedClinicalEntrySequenceNumber14",
                "EmergencyCareDiagnosisQualifier14",
                "EmergencyCareDiagnosis15",
                "CodedClinicalEntrySequenceNumber15",
                "EmergencyCareDiagnosisQualifier15",
                "EmergencyCareDiagnosis16",
                "CodedClinicalEntrySequenceNumber16",
                "EmergencyCareDiagnosisQualifier16",
                "EmergencyCareDiagnosis17",
                "CodedClinicalEntrySequenceNumber17",
                "EmergencyCareDiagnosisQualifier17",
                "EmergencyCareDiagnosis18",
                "CodedClinicalEntrySequenceNumber18",
                "EmergencyCareDiagnosisQualifier18",
                "EmergencyCareDiagnosis19",
                "CodedClinicalEntrySequenceNumber19",
                "EmergencyCareDiagnosisQualifier19",
                "EmergencyCareDiagnosis20",
                "CodedClinicalEntrySequenceNumber20",
                "EmergencyCareDiagnosisQualifier20",
                "EmergencyCareClinicalInvestigation1",
                "EmergencyCareProcedureDate1",
                "EmergencyCareProcedureTime1",
                "EmergencyCareClinicalInvestigation2",
                "EmergencyCareProcedureDate2",
                "EmergencyCareProcedureTime2",
                "EmergencyCareClinicalInvestigation3",
                "EmergencyCareProcedureDate3",
                "EmergencyCareProcedureTime3",
                "EmergencyCareClinicalInvestigation4",
                "EmergencyCareProcedureDate4",
                "EmergencyCareProcedureTime4",
                "EmergencyCareClinicalInvestigation5",
                "EmergencyCareProcedureDate5",
                "EmergencyCareProcedureTime5",
                "EmergencyCareClinicalInvestigation6",
                "EmergencyCareProcedureDate6",
                "EmergencyCareProcedureTime6",
                "EmergencyCareClinicalInvestigation7",
                "EmergencyCareProcedureDate7",
                "EmergencyCareProcedureTime7",
                "EmergencyCareClinicalInvestigation8",
                "EmergencyCareProcedureDate8",
                "EmergencyCareProcedureTime8",
                "EmergencyCareClinicalInvestigation9",
                "EmergencyCareProcedureDate9",
                "EmergencyCareProcedureTime9",
                "EmergencyCareClinicalInvestigation10",
                "EmergencyCareProcedureDate10",
                "EmergencyCareProcedureTime10",
                "EmergencyCareClinicalInvestigation11",
                "EmergencyCareProcedureDate11",
                "EmergencyCareProcedureTime11",
                "EmergencyCareClinicalInvestigation12",
                "EmergencyCareProcedureDate12",
                "EmergencyCareProcedureTime12",
                "EmergencyCareClinicalInvestigation13",
                "EmergencyCareProcedureDate13",
                "EmergencyCareProcedureTime13",
                "EmergencyCareClinicalInvestigation14",
                "EmergencyCareProcedureDate14",
                "EmergencyCareProcedureTime14",
                "EmergencyCareClinicalInvestigation15",
                "EmergencyCareProcedureDate15",
                "EmergencyCareProcedureTime15",
                "EmergencyCareClinicalInvestigation16",
                "EmergencyCareProcedureDate16",
                "EmergencyCareProcedureTime16",
                "EmergencyCareClinicalInvestigation17",
                "EmergencyCareProcedureDate17",
                "EmergencyCareProcedureTime17",
                "EmergencyCareClinicalInvestigation18",
                "EmergencyCareProcedureDate18",
                "EmergencyCareProcedureTime18",
                "EmergencyCareClinicalInvestigation19",
                "EmergencyCareProcedureDate19",
                "EmergencyCareProcedureTime19",
                "EmergencyCareClinicalInvestigation20",
                "EmergencyCareProcedureDate20",
                "EmergencyCareProcedureTime20",
                "EmergencyCareTreatments1",
                "EmergencyCareTreatmentDate1",
                "EmergencyCareTreatmentTime1",
                "EmergencyCareTreatments2",
                "EmergencyCareTreatmentDate2",
                "EmergencyCareTreatmentTime2",
                "EmergencyCareTreatments3",
                "EmergencyCareTreatmentDate3",
                "EmergencyCareTreatmentTime3",
                "EmergencyCareTreatments4",
                "EmergencyCareTreatmentDate4",
                "EmergencyCareTreatmentTime4",
                "EmergencyCareTreatments5",
                "EmergencyCareTreatmentDate5",
                "EmergencyCareTreatmentTime5",
                "EmergencyCareTreatments6",
                "EmergencyCareTreatmentDate6",
                "EmergencyCareTreatmentTime6",
                "EmergencyCareTreatments7",
                "EmergencyCareTreatmentDate7",
                "EmergencyCareTreatmentTime7",
                "EmergencyCareTreatments8",
                "EmergencyCareTreatmentDate8",
                "EmergencyCareTreatmentTime8",
                "EmergencyCareTreatments9",
                "EmergencyCareTreatmentDate9",
                "EmergencyCareTreatmentTime9",
                "EmergencyCareTreatments10",
                "EmergencyCareTreatmentDate10",
                "EmergencyCareTreatmentTime10",
                "EmergencyCareTreatments11",
                "EmergencyCareTreatmentDate11",
                "EmergencyCareTreatmentTime11",
                "EmergencyCareTreatments12",
                "EmergencyCareTreatmentDate12",
                "EmergencyCareTreatmentTime12",
                "EmergencyCareTreatments13",
                "EmergencyCareTreatmentDate13",
                "EmergencyCareTreatmentTime13",
                "EmergencyCareTreatments14",
                "EmergencyCareTreatmentDate14",
                "EmergencyCareTreatmentTime14",
                "EmergencyCareTreatments15",
                "EmergencyCareTreatmentDate15",
                "EmergencyCareTreatmentTime15",
                "EmergencyCareTreatments16",
                "EmergencyCareTreatmentDate16",
                "EmergencyCareTreatmentTime16",
                "EmergencyCareTreatments17",
                "EmergencyCareTreatmentDate17",
                "EmergencyCareTreatmentTime17",
                "EmergencyCareTreatments18",
                "EmergencyCareTreatmentDate18",
                "EmergencyCareTreatmentTime18",
                "EmergencyCareTreatments19",
                "EmergencyCareTreatmentDate19",
                "EmergencyCareTreatmentTime19",
                "EmergencyCareTreatments20",
                "EmergencyCareTreatmentDate20",
                "EmergencyCareTreatmentTime20",
                "ReferredtoServiceSnomed1",
                "ActivityServiceRequestDate1",
                "ActivityServiceRequestTime1",
                "ReferraltoServiceAssessmentDate1",
                "ReferraltoServiceAssessmentTime1",
                "ReferredtoServiceSnomed2",
                "ActivityServiceRequestDate2",
                "ActivityServiceRequestTime2",
                "ReferraltoServiceAssessmentDate2",
                "ReferraltoServiceAssessmentTime2",
                "ReferredtoServiceSnomed3",
                "ActivityServiceRequestDate3",
                "ActivityServiceRequestTime3",
                "ReferraltoServiceAssessmentDate3",
                "ReferraltoServiceAssessmentTime3",
                "ReferredtoServiceSnomed4",
                "ActivityServiceRequestDate4",
                "ActivityServiceRequestTime4",
                "ReferraltoServiceAssessmentDate4",
                "ReferraltoServiceAssessmentTime4",
                "ReferredtoServiceSnomed5",
                "ActivityServiceRequestDate5",
                "ActivityServiceRequestTime5",
                "ReferraltoServiceAssessmentDate5",
                "ReferraltoServiceAssessmentTime5",
                "ReferredtoServiceSnomed6",
                "ActivityServiceRequestDate6",
                "ActivityServiceRequestTime6",
                "ReferraltoServiceAssessmentDate6",
                "ReferraltoServiceAssessmentTime6",
                "ReferredtoServiceSnomed7",
                "ActivityServiceRequestDate7",
                "ActivityServiceRequestTime7",
                "ReferraltoServiceAssessmentDate7",
                "ReferraltoServiceAssessmentTime7",
                "ReferredtoServiceSnomed8",
                "ActivityServiceRequestDate8",
                "ActivityServiceRequestTime8",
                "ReferraltoServiceAssessmentDate8",
                "ReferraltoServiceAssessmentTime8",
                "ReferredtoServiceSnomed9",
                "ActivityServiceRequestDate9",
                "ActivityServiceRequestTime9",
                "ReferraltoServiceAssessmentDate9",
                "ReferraltoServiceAssessmentTime9",
                "ReferredtoServiceSnomed10",
                "ActivityServiceRequestDate10",
                "ActivityServiceRequestTime10",
                "ReferraltoServiceAssessmentDate10",
                "ReferraltoServiceAssessmentTime10",
                "DecidedtoAdmitDate",
                "DecidedtoAdmitTime",
                "ActivityTreatmentFunctionCode",
                "EmergencyCareDischargeStatusSnomed",
                "EmergencyCareConclusionDate",
                "EmergencyCareConclusionTime",
                "EmergencyCareDepartureDate",
                "EmergencyCareDepartureTime",
                "SafeguardingConcernSnomed1",
                "SafeguardingConcernSnomed2",
                "SafeguardingConcernSnomed3",
                "SafeguardingConcernSnomed4",
                "SafeguardingConcernSnomed5",
                "SafeguardingConcernSnomed6",
                "SafeguardingConcernSnomed7",
                "SafeguardingConcernSnomed8",
                "SafeguardingConcernSnomed9",
                "SafeguardingConcernSnomed10",
                "EmergencyCareDischargeDestination",
                "OrganisationSiteIdentifierDischargeFromEmergencyCare",
                "EmergencyCareDischargeFollowup",
                "EmergencyCareDischargeInformationGiven",
                "ClinicalTrialIdentifier",
                "DiseaseOutbreakNotification"

        };
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        return getCsvHeadersForVersion(version);
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

}
