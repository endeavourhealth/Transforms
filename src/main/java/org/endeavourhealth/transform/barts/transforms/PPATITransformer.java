package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsCernerCodeValueRefDal;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsInternalIdDal;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPATI;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
import org.hl7.fhir.instance.model.Enumerations;
import org.hl7.fhir.instance.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PPATITransformer extends BartsBasisTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PPATITransformer.class);

    private static InternalIdDalI internalIdDalI = null;

    private static SimpleDateFormat formatDaily = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static SimpleDateFormat formatBulk = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");

    public static void transform(String version,
                                 PPATI parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry(parser);
                if (valStr == null) {
                    createPatient(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static String validateEntry(PPATI parser) {
        return null;
    }

    public static void createPatient(PPATI parser,
                                     FhirResourceFiler fhirResourceFiler,
                                     BartsCsvHelper csvHelper,
                                     String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {


        if (internalIdDalI == null) {
            internalIdDalI = DalProvider.factoryInternalIdDal();
        }

        //store the MRN/PersonID mapping
        CsvCell millenniumPersonIdCell = parser.getMillenniumPersonId();
        CsvCell mrnCell = parser.getLocalPatientId();
        internalIdDalI.upsertRecord(fhirResourceFiler.getServiceId(), RdbmsInternalIdDal.IDTYPE_MRN_MILLENNIUM_PERS_ID,
                                    mrnCell.getString(), millenniumPersonIdCell.getString());


        CsvCell millenniumPersonId = parser.getMillenniumPersonId();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(millenniumPersonId, csvHelper);

        //TODO - need to avoid duplicating Identifiers, Extensions, Communications etc. if we're processing a Delta extract

        if (!millenniumPersonId.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirUri.IDENTIFIER_SYSTEM_CERNER_INTERNAL_PERSON);
            identifierBuilder.setValue(millenniumPersonId.getString());
        }

        if (!mrnCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID);
            identifierBuilder.setValue(mrnCell.getString());
        }

        CsvCell nhsNumberCell = parser.getNhsNumber();
        if (!nhsNumberCell.isEmpty()) {
            String nhsNumber = nhsNumberCell.getString();
            nhsNumber = nhsNumber.trim();
            nhsNumber = nhsNumber.replace("-","");

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);

            if (nhsNumber.length() == 10) {
                identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
                identifierBuilder.setSystem(FhirUri.IDENTIFIER_SYSTEM_NHSNUMBER);
                identifierBuilder.setValue(nhsNumber);

            } else {
                //add the invalid NHS number as a secondary identifier
                identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
                identifierBuilder.setSystem(FhirUri.IDENTIFIER_SYSTEM_CERNER_INTERNAL_PERSON);
                identifierBuilder.setValue(nhsNumber);
            }
        }

        CsvCell nhsNumberStatusCell = parser.getNhsNumberStatus();
        if (!nhsNumberStatusCell.isEmpty()) {

            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                                                                        RdbmsCernerCodeValueRefDal.NHS_NUMBER_STATUS,
                                                                        nhsNumberStatusCell.getLong(),
                                                                        fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                String cernerDesc = cernerCodeValueRef.getCodeDescTxt();
                NhsNumberVerificationStatus verificationStatus = convertNhsNumberVeriticationStatus(cernerDesc);
                patientBuilder.setNhsNumberVerificationStatus(verificationStatus, nhsNumberStatusCell);

            } else {
                // LOG.warn("NHS Status code: " + parser.getActiveIndicator() + " not found in Code Value lookup");
            }
        }

        CsvCell activeCell = parser.getActiveIndicator();
        patientBuilder.setActive(activeCell.getIntAsBoolean(), activeCell);

        CsvCell dateOfBirthCell = parser.getDateOfBirth();
        if (!dateOfBirthCell.isEmpty()) {
            //we need to handle multiple formats, so attempt to apply both formats here
            Date dob = null;
            try {
                dob = formatDaily.parse(dateOfBirthCell.getString());
            } catch (ParseException ex) {
                dob = formatBulk.parse(dateOfBirthCell.getString());
            }
            patientBuilder.setDateOfBirth(dob, dateOfBirthCell);
        }

        CsvCell genderCell = parser.getGenderCode();
        if (!genderCell.isEmpty()) {
            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                                                                                RdbmsCernerCodeValueRefDal.GENDER,
                                                                                genderCell.getLong(),
                                                                                fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                Enumerations.AdministrativeGender gender = SexConverter.convertCernerSexToFhir(cernerCodeValueRef.getCodeMeaningTxt());
                patientBuilder.setGender(gender, genderCell);
            } else {
                // LOG.warn("Gender code: " + parser.getGenderCode() + " not found in Code Value lookup");
            }
        }

        CsvCell maritalStatusCode = parser.getMaritalStatusCode();
        if (!maritalStatusCode.isEmpty()) {
            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                                                                        RdbmsCernerCodeValueRefDal.MARITAL_STATUS,
                                                                        maritalStatusCode.getLong(),
                                                                        fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                MaritalStatus maritalStatus = convertMaritalStatus(cernerCodeValueRef.getCodeMeaningTxt());
                patientBuilder.setMaritalStatus(maritalStatus, maritalStatusCode);

            } else {
                // LOG.warn("Marital Status code: " + parser.getMaritalStatusCode() + " not found in Code Value lookup");
            }
        }

        CsvCell ethnicityCode = parser.getEthnicGroupCode();
        if (!ethnicityCode.isEmpty()) {
            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                                                                            RdbmsCernerCodeValueRefDal.ETHNIC_GROUP,
                                                                            ethnicityCode.getLong(),
                                                                            fhirResourceFiler.getServiceId());

            EthnicCategory ethnicCategory = convertEthnicCategory(cernerCodeValueRef.getAliasNhsCdAlias());
            patientBuilder.setEthnicity(ethnicCategory, ethnicityCode);
        }

        CsvCell languageCell = parser.getFirstLanguageCode();
        if (!languageCell.isEmpty()) {

            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                                                                            RdbmsCernerCodeValueRefDal.LANGUAGE,
                                                                            languageCell.getLong(),
                                                                            fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                String codeTerm = cernerCodeValueRef.getCodeDescTxt();

                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(patientBuilder, PatientBuilder.TAG_CODEABLE_CONCEPT_LANGUAGE);
                codeableConceptBuilder.addCoding(FhirUri.CODE_SYSTEM_CERNER_CODE_ID);
                codeableConceptBuilder.setCodingCode(languageCell.getString(), languageCell);
                codeableConceptBuilder.setCodingDisplay(codeTerm);

            } else {
                // LOG.warn("Language code: " + parser.getFirstLanguageCode() + " not found in Code Value lookup");
            }
        }

        CsvCell religionCell = parser.getReligionCode();
        if (!religionCell.isEmpty()) {
            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                                                                    RdbmsCernerCodeValueRefDal.RELIGION,
                                                                    religionCell.getLong(),
                                                                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                String codeTerm = cernerCodeValueRef.getCodeDescTxt();

                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(patientBuilder, PatientBuilder.TAG_CODEABLE_CONCEPT_LANGUAGE);
                codeableConceptBuilder.addCoding(FhirUri.CODE_SYSTEM_CERNER_CODE_ID);
                codeableConceptBuilder.setCodingCode(religionCell.getString(), religionCell);
                codeableConceptBuilder.setCodingDisplay(codeTerm);

            } else {
                // LOG.warn("Religion code: " + parser.getReligionCode() + " not found in Code Value lookup");
            }
        }

        // If we have a deceased date, set that but if not and the patient is deceased just set the deceased flag
        CsvCell deceasedDateTimeCell = parser.getDeceasedDateTime();
        CsvCell deceasedMethodCell = parser.getDeceasedMethodCode();
        if (!deceasedDateTimeCell.isEmpty()) {

            //could be in one of two format
            Date dod = null;
            try {
                dod = formatDaily.parse(deceasedDateTimeCell.getString());
            } catch (ParseException ex) {
                dod = formatBulk.parse(deceasedDateTimeCell.getString());
            }

            patientBuilder.setDateOfDeath(dod, deceasedDateTimeCell);

        } else if (!deceasedMethodCell.isEmpty()) {

            String code = deceasedMethodCell.getString();
            if (!code.equals("0") &&
                    !code.equals("684730")) {

                patientBuilder.setDateOfDeathBoolean(true, deceasedMethodCell);
            }
        }
    }

    private static EthnicCategory convertEthnicCategory(String aliasNhsCdAlias) {

        //the alias field on the Cerner code ref table matches the NHS Data Dictionary ethnicity values
        //except for 99, whcih means "not stated"
        if (aliasNhsCdAlias.equalsIgnoreCase("99")) {
            return EthnicCategory.NOT_STATED;

        } else {
            return EthnicCategory.fromCode(aliasNhsCdAlias);
        }
    }

    /*public static void createPatient(PPATI parser,
                                     FhirResourceFiler fhirResourceFiler,
                                     String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {


        if (internalIdDalI == null) {
            internalIdDalI = DalProvider.factoryInternalIdDal();
        }

        if (cernerCodeValueRefDalI == null) {
            cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
        }

        internalIdDalI.upsertRecord(fhirResourceFiler.getServiceId(), RdbmsInternalIdDal.IDTYPE_MRN_MILLENNIUM_PERS_ID,
                parser.getLocalPatientId(), parser.getMillenniumPersonId());

        ResourceId patientResourceId = getPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, parser.getMillenniumPersonId());
        if (patientResourceId == null && !parser.isActive()) {
            //We don't already have the patient and it is not active so ignore it.
            return;
        }

        if (patientResourceId == null) {
            patientResourceId = createPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, parser.getMillenniumPersonId());
        }

        Patient fhirPatient = new Patient();
        fhirPatient.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PATIENT));

        fhirPatient.setId(patientResourceId.getResourceId().toString());

        fhirPatient.addIdentifier(IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID,
                parser.getMillenniumPersonId()));

        if (!parser.isActive()) {
            // Patient is inactive and we already have them in the system so just set them to inactive and save the resource
            fhirPatient.setActive(false);
            PatientResourceCache.savePatientResource(Long.parseLong(parser.getMillenniumPersonId()), fhirPatient);
        }

        fhirPatient.addIdentifier(IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, FhirUri.IDENTIFIER_SYSTEM_CERNER_INTERNAL_PERSON,
                parser.getLocalPatientId()));

        String nhsNumber = parser.getNhsNumber();
        if (!Strings.isNullOrEmpty(nhsNumber)) {
            nhsNumber = nhsNumber.trim().replace("-","");
            if (nhsNumber.length() == 10) {
                fhirPatient.addIdentifier(IdentifierHelper.createNhsNumberIdentifier(nhsNumber));
            } else {
                //add the invalid NHS number as a secondary identifier
                fhirPatient.addIdentifier(IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY,
                        FhirUri.IDENTIFIER_SYSTEM_CERNER_INTERNAL_PERSON,
                        nhsNumber));
            }
        }

        if (parser.getNhsNumberStatus() != null && parser.getNhsNumberStatus().length() > 0) {

            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.NHS_NUMBER_STATUS,
                    Long.parseLong(parser.getActiveIndicator()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                CodeableConcept fhirCodeableConcept = null;
                //convert the String to one of the official statuses. If it can't be converted, insert free-text in the codeable concept
                NhsNumberVerificationStatus verificationStatus = convertNhsNumberVeriticationStatus(cernerCodeValueRef.getCodeDescTxt());
                if (verificationStatus != null) {
                    fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(verificationStatus);

                } else {
                    fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(cernerCodeValueRef.getCodeDescTxt());
                }
                fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_NHS_NUMBER_VERIFICATION_STATUS,
                        fhirCodeableConcept));
            } else {
                // LOG.warn("NHS Status code: " + parser.getActiveIndicator() + " not found in Code Value lookup");
            }
        }

        if (parser.getActiveIndicator() != null && parser.getActiveIndicator().length() > 0) {
            fhirPatient.setActive(parser.isActive());
        }

        if (!Strings.isNullOrEmpty(parser.getDateOfBirthAsString())) {
            Date dob = null;
            try {
                dob = formatDaily.parse(parser.getDateOfBirthAsString());
            } catch (ParseException ex) {
                dob = formatBulk.parse(parser.getDateOfBirthAsString());
            }
            fhirPatient.setBirthDate(dob);
        }

        if (parser.getGenderCode() != null && parser.getGenderCode().length() > 0) {
            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.GENDER,
                    Long.parseLong(parser.getGenderCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                Enumerations.AdministrativeGender gender = SexConverter.convertCernerSexToFhir(cernerCodeValueRef.getCodeMeaningTxt());
                fhirPatient.setGender(gender);
            } else {
                // LOG.warn("Gender code: " + parser.getGenderCode() + " not found in Code Value lookup");
            }
        }

        if (parser.getMaritalStatusCode() != null && parser.getMaritalStatusCode().length() > 0) {
            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.MARITAL_STATUS,
                    Long.parseLong(parser.getMaritalStatusCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                MaritalStatus maritalStatus = convertMaritalStatus(cernerCodeValueRef.getCodeMeaningTxt());
                if (maritalStatus != null) {
                    CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(maritalStatus);
                    fhirPatient.setMaritalStatus(codeableConcept);
                } else {
                    // LOG.warn("Marital Status code: " + cernerCodeValueRef.getCodeMeaningTxt() + " not found in status conversion code");
                }
            } else {
                // LOG.warn("Marital Status code: " + parser.getMaritalStatusCode() + " not found in Code Value lookup");
            }
        }

        if (parser.getEthnicGroupCode() != null && parser.getEthnicGroupCode().length() > 0) {
            CodeableConcept ethnicGroup = new CodeableConcept();
            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.ETHNIC_GROUP,
                    Long.parseLong(parser.getEthnicGroupCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                ethnicGroup.addCoding().setCode(parser.getEthnicGroupCode()).setSystem(FhirExtensionUri.PATIENT_ETHNICITY)
                        .setDisplay(cernerCodeValueRef.getCodeDescTxt());
            } else {
                // LOG.warn("Ethnic Group code: " + parser.getEthnicGroupCode() + " not found in Code Value lookup");
            }
        }

        if (parser.getFirstLanguageCode() != null && parser.getFirstLanguageCode().length() > 0) {
            CodeableConcept languageConcept = new CodeableConcept();
            Patient.PatientCommunicationComponent fhirCommunication = fhirPatient.addCommunication();

            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.LANGUAGE,
                    Long.parseLong(parser.getFirstLanguageCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                languageConcept.addCoding().setCode(parser.getFirstLanguageCode()).setSystem(FhirUri.CODE_SYSTEM_CERNER_CODE_ID)
                        .setDisplay(cernerCodeValueRef.getCodeDescTxt());

                fhirCommunication.setLanguage(languageConcept);
                fhirCommunication.setPreferred(true);

                fhirPatient.addCommunication(fhirCommunication);
            } else {
                // LOG.warn("Language code: " + parser.getFirstLanguageCode() + " not found in Code Value lookup");
            }
        }

        if (parser.getReligionCode() != null && parser.getReligionCode().length() > 0) {
            CodeableConcept religionConcept = new CodeableConcept();
            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.RELIGION,
                    Long.parseLong(parser.getReligionCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                religionConcept.addCoding().setCode(parser.getReligionCode()).setSystem(FhirUri.CODE_SYSTEM_CERNER_CODE_ID)
                        .setDisplay(cernerCodeValueRef.getCodeDescTxt());

                fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_RELIGION, religionConcept));
            } else {
                // LOG.warn("Religion code: " + parser.getReligionCode() + " not found in Code Value lookup");
            }
        }

        // If we have a deceased date, set that but if not and the patient is deceased just set the deceased flag
        if (!Strings.isNullOrEmpty(parser.getDeceasedDateTimeAsString()) || parser.getDeceasedMethodCode() != null) {
            if (!Strings.isNullOrEmpty(parser.getDeceasedDateTimeAsString())) {
                Date dod = null;
                try {
                    dod = formatDaily.parse(parser.getDeceasedDateTimeAsString());
                } catch (ParseException ex) {
                    dod = formatBulk.parse(parser.getDeceasedDateTimeAsString());
                }
                fhirPatient.setDeceased(new DateTimeType(dod));
            }  // 684730 = No for deceased method!
            else if (!parser.getDeceasedMethodCode().equals("0") || !parser.getDeceasedMethodCode().equals("684730")) {
                fhirPatient.setDeceased(new BooleanType(true));
            }
        }


        PatientResourceCache.savePatientResource(Long.parseLong(parser.getMillenniumPersonId()), fhirPatient);


        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");

        //ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);


    }*/

    private static NhsNumberVerificationStatus convertNhsNumberVeriticationStatus(String nhsNumberStatus) {
        switch (nhsNumberStatus) {
            case "Present and verified":
                return NhsNumberVerificationStatus.PRESENT_AND_VERIFIED;
            case "Needs Resolution":
                return NhsNumberVerificationStatus.TRACE_NEEDS_TO_BE_RESOLVED;
            case "Present not Traced":
                return NhsNumberVerificationStatus.PRESENT_BUT_NOT_TRACED;
            case "Traced Attempted":
                return NhsNumberVerificationStatus.TRACE_ATTEMPTED_NO_MATCH;
            case "Traced Required":
                return NhsNumberVerificationStatus.TRACE_REQUIRED;
            case "Trace in Progress":
                return NhsNumberVerificationStatus.TRACE_IN_PROGRESS;
            case "Trace not Required":
                return NhsNumberVerificationStatus.NUMBER_NOT_PRESENT_NO_TRACE_REQUIRED;
            case "Trace Postponed":
                return NhsNumberVerificationStatus.TRACE_POSTPONED;
            default:
                throw new IllegalArgumentException("Unmapped NHS number status [" + nhsNumberStatus + "]");
        }
    }

    private static MaritalStatus convertMaritalStatus(String statusCode) {
        switch (statusCode) {
            case "DIVORCED": return MaritalStatus.DIVORCED;
            case "MARRIED": return MaritalStatus.MARRIED;
            case "LGL_SPRTN": return MaritalStatus.LEGALLY_SEPARATED;
            case "SINGLE": return MaritalStatus.NEVER_MARRIED;
            case "UNKNOWN": return null;
            case "WIDOW": return MaritalStatus.WIDOWED;
            case "LIFE_PTNR": return MaritalStatus.DOMESTIC_PARTNER;
            default:
                throw new IllegalArgumentException("Unmapped marital status [" + statusCode + "]");
        }
    }
}
