package org.endeavourhealth.transform.barts.schema;

public class EmergencyCareDataSetStatics {
    public static final String[] V1_DATA_SET = new String[]{
            "CDSTypeCode",
            "CDSProtocolType",
            "BulkReplacementGroup",
            "CDSUniqueIdentifier",                                          //staging
            "CDSUpdateType",                                                //staging
            "CDSApplicableDate",
            "CDSApplicableTime",
            "CDSExtractDate",
            "CDSExtractTime",
            "CDSReportStartDate",
            "CDSReportEndDate",
            "CDSActivityDate",                                              //staging
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
            "PatientPathwayIdentifier",                                     //staging
            "OrganisationCodePatientIdentifierIssuer",
            "ReferraltoTreatmentPeriodStatus",
            "WaitingTimeMeasurementType",
            "ReferraltoTreatmentPeriodStartDate",
            "ReferraltoTreatmentPeriodEndDate",
            "WithheldReason",                                               //staging (if set - withHeldFlag = 1)
            "NHSNumber",                                                    //staging
            "NHSNumberStatus",
            "LocalPatientID",                                               //staging
            "OrganisationCodeLocalPatientID",
            "PersonFullName",
            "PersonTitle",
            "PersonGivenName",
            "PersonFamilyName",
            "PersonNameSuffix",
            "PersonInitials",
            "DateofBirth",                                                  //staging
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
            "StartDateMHALegalStatusClassificationAssignmentPeriod1",   //staging in single datetime with below
            "StartTimeMHALegalStatusClassificationAssignmentPeriod1",   //staging
            "ExpiryDateMHALegalStatusClassification1",                  //staging in single datetime with below
            "ExpiryTimeMHALegalStatusClassification1",                  //staging
            "MHALegalStatusClassificationCode1",                        //staging (column to hold all 10 delimetered with datetime pieces)
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
            "OrganisationSiteIdentifierofTreatment",                            //staging
            "EmergencyCareDepartmentType",                                      //staging
            "AmbulanceIncidentNumber",                                          //staging
            "OrganisationCodeConveyingAmbulanceTrust",
            "EmergencyCareAttendanceIdentifier",                                //staging
            "EmergencyCareArrivalMode",                                         //staging
            "EmergencyCareAttendanceCategory",                                  //staging
            "EmergencyCareAttendanceSource",                                    //staging
            "EmergencyCareOrganisationCodeAttendanceSource",
            "EmergencyCareArrivalDate",                                         //staging
            "EmergencyCareArrivalTime",                                         //staging (derived with above)
            "EmergencyCareAgeatCDSActivityDate",
            "EmergencyCareInitialAssessmentDate",                               //staging
            "EmergencyCareInitialAssessmentTime",                               //staging (derived with above)
            "EmergencyCareAcuitySnomed",
            "EmergencyCareChiefComplaint",                                      //staging
            "EmergencyCareDateSeenforTreatment",                                //staging
            "EmergencyCareTimeSeenforTreatment",                                //staging (derived with above)
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
            "EmergencyCareDiagnosis1",                                          //staging (column to hold all 20 delimetered)
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
            "EmergencyCareClinicalInvestigation1",                              //staging (column to hold all 20 delimetered with datetime piece)
            "EmergencyCareProcedureDate1",                                      //staging
            "EmergencyCareProcedureTime1",                                      //staging
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
            "EmergencyCareTreatments1",                                     //staging (column to hold all 20 delimetered with datetime piece)
            "EmergencyCareTreatmentDate1",                                  //staging
            "EmergencyCareTreatmentTime1",                                  //staging
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
            "ReferredtoServiceSnomed1",                                     //staging (column to hold all 10 delimetered with datetime pieces)
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
            "DecidedtoAdmitDate",                                           //staging
            "DecidedtoAdmitTime",                                           //staging
            "ActivityTreatmentFunctionCode",                                //staging
            "EmergencyCareDischargeStatusSnomed",                           //staging
            "EmergencyCareConclusionDate",                                  //staging
            "EmergencyCareConclusionTime",                                  //staging (derived with above)
            "EmergencyCareDepartureDate",                                   //staging
            "EmergencyCareDepartureTime",                                   //staging (derived with above)
            "SafeguardingConcernSnomed1",                                   //staging (column to hold all 10 delimetered)
            "SafeguardingConcernSnomed2",
            "SafeguardingConcernSnomed3",
            "SafeguardingConcernSnomed4",
            "SafeguardingConcernSnomed5",
            "SafeguardingConcernSnomed6",
            "SafeguardingConcernSnomed7",
            "SafeguardingConcernSnomed8",
            "SafeguardingConcernSnomed9",
            "SafeguardingConcernSnomed10",
            "EmergencyCareDischargeDestination",                            //staging
            "OrganisationSiteIdentifierDischargeFromEmergencyCare",         //staging
            "EmergencyCareDischargeFollowup",                               //staging
            "EmergencyCareDischargeInformationGiven",
            "ClinicalTrialIdentifier",
            "DiseaseOutbreakNotification"
    };

    public static final String[] V2_DATA_SET = new String[]{
            "CDSTypeCode",
            "CDSProtocolType",
            "BulkReplacementGroup",
            "CDSUniqueIdentifier",                                          //staging
            "CDSUpdateType",                                                //staging
            "CDSApplicableDate",
            "CDSApplicableTime",
            "CDSExtractDate",
            "CDSExtractTime",
            "CDSReportStartDate",
            "CDSReportEndDate",
            "CDSActivityDate",                                              //staging
            "OrganisationCodeSender",
            "OrganisationCodeCopyRecipient1",
            "OrganisationCodeCopyRecipient2",
            "OrganisationCodeCopyRecipient3",
            "OrganisationCodeCopyRecipient4",
            "OrganisationCodeCopyRecipient5",
            "OrganisationCodeCopyRecipient6",
            "OrganisationCodeCopyRecipient7",
             //Service Agreement Details
            "CommissioningSerialNumber",
            "NHSServiceAgreementLineNumber",
            "ProviderReferenceNumber",
            "CommissioningReferenceNumber",
            "OrganisationCodeCodeofCommissioner",
            "OrganisationCodeCodeofProvider",
            //Patient Pathway
            "UniqueBookingReferencenumber",
            "PatientPathwayIdentifier",                                     //staging
            "OrganisationCodePatientIdentifierIssuer",
            "ReferraltoTreatmentPeriodStatus",
            "WaitingTimeMeasurementType",
            "ReferraltoTreatmentPeriodStartDate",
            "ReferraltoTreatmentPeriodEndDate",
            //Patient Identity
            "WithheldReason",                                               //staging (if set - withHeldFlag = 1)
            "NHSNumber",                                                    //staging
            "NHSNumberStatus",
            "LocalPatientID",                                               //staging
            "OrganisationCodeLocalPatientID",
            "PersonFullName",
            "PersonTitle",
            "PersonGivenName",
            "PersonFamilyName",
            "PersonNameSuffix",
            "PersonInitials",
            "DateofBirth",                                                  //staging
            "PatientUnstructuredAddress",
            "PatientStructuredAddressline1",
            "PatientStructuredAddressline2",
            "PatientStructuredAddressline3",
            "PatientStructuredAddressline4",
            "PatientStructuredAddressline5",
            "Postcode",
            "OrganisationCodeResidenceResponsibility",
            //Patient Characteristics
            "PersonCurrentGender",
            "EthnicCategory",
            "EthnicCategory2021",                                       //V2 exclusive field name
            "AccommodationStatusSnomed",
            "PreferredSpokenLanguage",
            "AccessibleInformationProfessionalRequiredCodeSNOMEDCT",
            "InterpreterLanguageSNOMEDCT",
            "OverseasVisitorChargingCategoryatCDSActivityDate",
            //Mental Health act Legal Status
            "MHALegalStatusColumn",                        //Mental health repeating group field
            //GP Registration
            "GeneralMedicalPractionerSpecified",
            "GeneralMedicalPracticeCodePatientRegistration",
            //Emergency Care Location Group
            "OrganisationSiteIdentifierofTreatment",                            //staging
            "EmergencyCareDepartmentType",                                      //staging
            //Ambulance
            "AmbulanceCallIdentifier",                                          //staging
            "OrganisationCodeConveyingAmbulanceTrust",
            "CareContactIdentifierAmbulanceService",                            //unique to V2
            //Expected Date and Time of Treatment
            "EmergencyCareExpectedCol",                //repeating group unique to v2
            //Attendance
            "EmergencyCareAttendanceIdentifier",                                //staging
            "EmergencyCareArrivalMode",                                         //staging
            "EmergencyCareAttendanceCategory",                                  //staging
            "EmergencyCareAttendanceSource",                                    //staging
            "EmergencyCareOrganisationCodeAttendanceSource",
            "EmergencyCareArrivalDate",                                         //staging
            "EmergencyCareArrivalTime",                                         //staging (derived with above)
            "EmergencyCareAgeAtCDSActivityDate",
            "EmergencyCareInitialAssessmentDate",                               //staging
            "EmergencyCareInitialAssessmentTime",                               //staging (derived with above)
            "EmergencyCareAcuitySnomed",
            "EmergencyCareChiefComplaint",                                      //staging
            "EmergencyCareDateSeenforTreatment",                                //staging
            "EmergencyCareTimeSeenforTreatment",//staging (derived with above)
            //coded score assessment
            "CodedScoreAssessmentCol",                                         //coded score assessment repeating group unique to v2
            //coded clinical observations repeating group unique to v2
            "CodedObservationCol",
            //Coded Clinical Findings - Repeating Group unique to V2
            "CodedFindingCol",
            //injury characteristics
            "InjuryCharacteristicsInjuryDate",
            "InjuryCharacteristicsInjuryTime",
            "InjuryCharacteristicsPlaceofInjury",
            "InjuryCharacteristicsPlaceofInjuryLatitude",
            "InjuryCharacteristicsPlaceofInjuryLongitude",
            "InjuryCharacteristicsInjuryIntent",
            "InjuryCharacteristicsActivityStatus",
            "InjuryCharacteristicsActivityType",
            "InjuryCharacteristicsActivityMechanism",
            //alcohol and drug involvement repeating group
            "InjuryCharacteristicsActivityAlcoholandDrugInvolvementCol",
            "AssaultLocationDescription",
            //comorbidities
            "InjuryCharacteristicsComorbiditiesCol",                                 //repeating group for comorbidities
            //repeating group Care Professionals Emergency Care
            "CareProfessionalRegistrationCol",
            //emergency care diagnosis
            "EmergencyCareDiagnosis",                                                   //staging (column to hold all 20 delimetered)<need clarity> emergency care diagnosis repeating group
            //emergency care investigation
            "EmergencyCareClinicalInvestigation",                                       //start of repeating group
            //Emergency care treatments
            "EmergencyCareTreatments",                                                  //start of repeating group
            //Referrals to other services
            "ReferredtoService",                                                  //start of repeating group
            //discharge from emergency care
            "DecidedtoAdmitDate",                                                       //staging
            "DecidedtoAdmitTime",                                                       //staging
            "ActivityTreatmentFunctionCode",                                            //staging
            "EmergencyCareClinicallyReadytoProceedTimestamp",                           //field unique to v2
            "EmergencyCareDischargeStatusSnomed",                                       //staging
            "EmergencyCareDepartureDate",                                               //staging
            "EmergencyCareDepartureTime",                                               //staging (derived with above)
            //Safeguarding concern
            "SafeguardingConcern",                                                //repeating group#
            //discharge from emergency care
            "EmergencyCareDischargeDestination",
            "OrganisationSiteIdentifierDischargeFromEmergencyCare",                     //staging
            "EmergencyCareDischargeFollowup",                                           //staging
            "EmergencyCareDischargeInformationGiven",
            //Research and disease outbreak notification
            "ClinicalTrialIdentifier",
            "DiseaseOutbreakNotification",
            "DiseaseOutbreakNotificationDescription"
    };
}
