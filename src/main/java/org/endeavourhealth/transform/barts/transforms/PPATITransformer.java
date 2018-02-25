package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
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

import javax.xml.crypto.dsig.TransformException;
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

        CsvCell millenniumPersonIdCell = parser.getMillenniumPersonId();
        CsvCell mrnCell = parser.getLocalPatientId();

        //store the MRN/PersonID mapping in BOTH directions
        internalIdDalI.upsertRecord(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID,
                                    mrnCell.getString(), millenniumPersonIdCell.getString());

        internalIdDalI.upsertRecord(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN,
                                    millenniumPersonIdCell.getString(), mrnCell.getString());

        CsvCell millenniumPersonId = parser.getMillenniumPersonId();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(millenniumPersonId, csvHelper);

        if (patientBuilder == null) {
            throw new TransformException("Failed to find patient builder for Person ID " + millenniumPersonId.getString() + " and MRN " + mrnCell.getString());
        }

        //because we may be processing a delta record on an existing patient resource, make sure to remove all these identifiers,
        //so they can be added back on without duplicating them
        IdentifierBuilder.removeExistingIdentifierBySystem(patientBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);
        IdentifierBuilder.removeExistingIdentifierBySystem(patientBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_INTERNAL_PERSON);
        IdentifierBuilder.removeExistingIdentifierBySystem(patientBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID);

        if (!millenniumPersonId.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_INTERNAL_PERSON);
            identifierBuilder.setValue(millenniumPersonId.getString(), millenniumPersonId);
        }

        if (!mrnCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID);
            identifierBuilder.setValue(mrnCell.getString(), mrnCell);
        }

        CsvCell nhsNumberCell = parser.getNhsNumber();
        if (!nhsNumberCell.isEmpty()) {
            String nhsNumber = nhsNumberCell.getString();
            nhsNumber = nhsNumber.replace("-",""); //Cerner NHS numbers are tokenised with hyphens, so remove

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);
            identifierBuilder.setValue(nhsNumber, nhsNumberCell);
        }

        CsvCell nhsNumberStatusCell = parser.getNhsNumberStatus();
        if (!nhsNumberStatusCell.isEmpty() && nhsNumberStatusCell.getLong() > 0) {

            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookUpCernerCodeFromCodeSet(
                                                                        CernerCodeValueRef.NHS_NUMBER_STATUS,
                                                                        nhsNumberStatusCell.getLong());

            String cernerDesc = cernerCodeValueRef.getCodeDescTxt();
            NhsNumberVerificationStatus verificationStatus = convertNhsNumberVeriticationStatus(cernerDesc);
            patientBuilder.setNhsNumberVerificationStatus(verificationStatus, nhsNumberStatusCell);

        } else {
            //we may be updating a patient, so make sure to remove if not set
            patientBuilder.setNhsNumberVerificationStatus(null);
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

        } else {
            //we may be updating an existing patient
            patientBuilder.setDateOfBirth(null);
        }

        CsvCell genderCell = parser.getGenderCode();
        if (!genderCell.isEmpty() && genderCell.getLong() > 0) {
            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookUpCernerCodeFromCodeSet(
                                                                                CernerCodeValueRef.GENDER,
                                                                                genderCell.getLong());

            Enumerations.AdministrativeGender gender = SexConverter.convertCernerSexToFhir(cernerCodeValueRef.getCodeMeaningTxt());
            patientBuilder.setGender(gender, genderCell);

        } else {
            //if updating a record then clear the gender if the field is empty
            patientBuilder.setGender(null);
        }

        CsvCell maritalStatusCode = parser.getMaritalStatusCode();
        if (!maritalStatusCode.isEmpty() && maritalStatusCode.getLong() > 0) {
            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookUpCernerCodeFromCodeSet(
                                                                        CernerCodeValueRef.MARITAL_STATUS,
                                                                        maritalStatusCode.getLong());

            MaritalStatus maritalStatus = convertMaritalStatus(cernerCodeValueRef.getCodeMeaningTxt());
            patientBuilder.setMaritalStatus(maritalStatus, maritalStatusCode);

        } else {
            //if updating a record, make sure to clear the field in this case
            patientBuilder.setMaritalStatus(null);
        }

        CsvCell ethnicityCode = parser.getEthnicGroupCode();
        if (!ethnicityCode.isEmpty() && ethnicityCode.getLong() > 0) {
            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookUpCernerCodeFromCodeSet(
                                                                            CernerCodeValueRef.ETHNIC_GROUP,
                                                                            ethnicityCode.getLong());

            EthnicCategory ethnicCategory = convertEthnicCategory(cernerCodeValueRef.getAliasNhsCdAlias());
            patientBuilder.setEthnicity(ethnicCategory, ethnicityCode);
        } else {
            //if this field is empty we should clear the value from the patient
            patientBuilder.setEthnicity(null);
        }

        CodeableConceptBuilder.removeExistingCodeableConcept(patientBuilder, PatientBuilder.TAG_CODEABLE_CONCEPT_LANGUAGE);
        CodeableConceptBuilder.removeExistingCodeableConcept(patientBuilder, PatientBuilder.TAG_CODEABLE_CONCEPT_RELIGION);

        CsvCell languageCell = parser.getFirstLanguageCode();
        BartsCodeableConceptHelper.applyCodeDescTxt(languageCell, CernerCodeValueRef.LANGUAGE, patientBuilder, PatientBuilder.TAG_CODEABLE_CONCEPT_LANGUAGE, csvHelper);

        CsvCell religionCell = parser.getReligionCode();
        BartsCodeableConceptHelper.applyCodeDescTxt(religionCell, CernerCodeValueRef.RELIGION, patientBuilder, PatientBuilder.TAG_CODEABLE_CONCEPT_RELIGION, csvHelper);

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

        } else {
            //if updating a record, we may have REMOVED a date of death set incorrectly, so clear the fields on the patient
            patientBuilder.clearDateOfDeath();
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


    private static NhsNumberVerificationStatus convertNhsNumberVeriticationStatus(String nhsNumberStatus) {

        //we've got at least one missing code, so return null
        if (nhsNumberStatus == null) {
            return null;
        }

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
