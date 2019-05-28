package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.common.fhir.schema.Religion;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PPATI;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
import org.hl7.fhir.instance.model.Enumerations;
import org.hl7.fhir.instance.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class PPATITransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPATITransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {

                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
                    continue;
                }

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                createPatient((PPATI) parser, fhirResourceFiler, csvHelper);
            }
        }
    }

    public static void createPatient(PPATI parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //this transform always UPDATES resources when possible, so we use the patient cache to retrieve from the DB
        CsvCell millenniumPersonIdCell = parser.getMillenniumPersonId();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(millenniumPersonIdCell);
        if (patientBuilder == null) {
            return;
        }

        //if the PPATI record is marked as non-active, it means we should delete the patient. When a merge is performed
        //in Cerner, the outgoing PPATI record is marked as non-active and all dependent records (e.g. ENCNT etc.) are moved
        //to point to the new Person ID. So we only need to delete the Patient resource and EpisodeOfCare (since we
        //artificially create them)
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            patientBuilder.setDeletedAudit(activeCell);
            csvHelper.getPatientCache().deletePatient(patientBuilder, millenniumPersonIdCell, fhirResourceFiler, parser.getCurrentState());
            return;
        }

        try {

            //let the PPALI transform add the MRN identifier, since the MRN in this file can be duplicated (e.g. MRN 9571132) and doesn't contain any start/end info
            /*CsvCell mrnCell = parser.getLocalPatientId();
            if (!mrnCell.isEmpty()) {
                addOrUpdateIdentifier(patientBuilder, mrnCell.getString(), mrnCell, Identifier.IdentifierUse.SECONDARY, FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID);
            }*/

            CsvCell nhsNumberCell = parser.getNhsNumber();
            if (!nhsNumberCell.isEmpty()) {
                String nhsNumber = nhsNumberCell.getString();
                nhsNumber = nhsNumber.replace("-", ""); //Cerner NHS numbers are tokenised with hyphens, so remove

                addOrUpdateIdentifier(patientBuilder, nhsNumber, nhsNumberCell, Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);
            }

            CsvCell nhsNumberStatusCell = parser.getNhsNumberStatus();
            if (!BartsCsvHelper.isEmptyOrIsZero(nhsNumberStatusCell)) {

                CsvCell cernerDescCell = BartsCodeableConceptHelper.getCellDesc(csvHelper, CodeValueSet.NHS_NUMBER_STATUS, nhsNumberStatusCell);
                if (cernerDescCell == null) {
                    TransformWarnings.log(LOG, parser, "ERROR: cerner code {} for PPATI {} not found", nhsNumberStatusCell, millenniumPersonIdCell);

                } else {
                    NhsNumberVerificationStatus verificationStatus = convertNhsNumberVeriticationStatus(cernerDescCell.getString(), parser);
                    patientBuilder.setNhsNumberVerificationStatus(verificationStatus, nhsNumberStatusCell, cernerDescCell);
                }

            } else {
                //we may be updating a patient, so make sure to remove if not set
                patientBuilder.setNhsNumberVerificationStatus(null);
            }

            CsvCell dateOfBirthCell = parser.getDateOfBirth();
            if (!BartsCsvHelper.isEmptyOrIsEndOfTime(dateOfBirthCell)) {
                //we need to handle multiple formats, so attempt to apply both formats here
                Date dob = BartsCsvHelper.parseDate(dateOfBirthCell);
                patientBuilder.setDateOfBirth(dob, dateOfBirthCell);

            } else {
                //we may be updating an existing patient
                patientBuilder.setDateOfBirth(null);
            }

            CsvCell genderCell = parser.getGenderCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(genderCell)) {
                CsvCell genderMeaningCell = BartsCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.GENDER, genderCell);
                if (genderMeaningCell == null) {
                    TransformWarnings.log(LOG, parser, "ERROR: cerner gender code {} not found", genderCell);

                } else {
                    Enumerations.AdministrativeGender gender = SexConverter.convertCernerSexToFhir(genderMeaningCell.getString());
                    patientBuilder.setGender(gender, genderCell, genderMeaningCell);
                }

            } else {
                //if updating a record then clear the gender if the field is empty
                patientBuilder.setGender(null);
            }

            CsvCell maritalStatusCode = parser.getMaritalStatusCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(maritalStatusCode)) {

                CsvCell maritalMeaningCell = BartsCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.MARITAL_STATUS, maritalStatusCode);
                if (maritalMeaningCell == null) {
                    TransformWarnings.log(LOG, parser, "ERROR: cerner marital status {} not found", maritalStatusCode);

                } else {
                    MaritalStatus maritalStatus = convertMaritalStatus(maritalMeaningCell.getString(), parser);
                    patientBuilder.setMaritalStatus(maritalStatus, maritalStatusCode, maritalMeaningCell);
                }
            } else {
                //if updating a record, make sure to clear the field in this case
                patientBuilder.setMaritalStatus(null);
            }

            CsvCell ethnicityCode = parser.getEthnicGroupCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(ethnicityCode)) {

                CsvCell ehtnicityCell = BartsCodeableConceptHelper.getCellAlias(csvHelper, CodeValueSet.ETHNIC_GROUP, ethnicityCode);
                if (ehtnicityCell == null) {
                    TransformWarnings.log(LOG, parser, "ERROR: cerner ethnicity {} not found", ethnicityCode);

                } else {
                    EthnicCategory ethnicCategory = convertEthnicCategory(ehtnicityCell.getString());
                    patientBuilder.setEthnicity(ethnicCategory, ethnicityCode, ehtnicityCell);
                }

            } else {
                //if this field is empty we should clear the value from the patient
                patientBuilder.setEthnicity(null);
            }

            //language

            //since we're working on an existing Patient resource we need to remove any existing language
            CodeableConceptBuilder.removeExistingCodeableConcept(patientBuilder, CodeableConceptBuilder.Tag.Patient_Language, null);

            CsvCell languageCell = parser.getFirstLanguageCode();
            BartsCodeableConceptHelper.applyCodeDescTxt(languageCell, CodeValueSet.LANGUAGE, patientBuilder, CodeableConceptBuilder.Tag.Patient_Language, csvHelper);

            //religion
            CsvCell religionCell = parser.getReligionCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(religionCell)) {
                CernerCodeValueRef cvref = csvHelper.lookupCodeRef(CodeValueSet.RELIGION, religionCell);

                //if possible, map the religion to the NHS data dictionary values
                Religion fhirReligion = mapReligion(cvref);
                if (fhirReligion != null) {
                    patientBuilder.setReligion(fhirReligion, religionCell);
                } else {
                    //if not possible to map, carry the value over as free text
                    String freeTextReligion = cvref.getCodeDispTxt();
                    patientBuilder.setReligionFreeText(freeTextReligion, religionCell);
                }

            } else {
                //it's an existing patient resource, so set to null as the religion field may have been cleared
                patientBuilder.setReligion(null);
            }
            /*CodeableConceptBuilder.removeExistingCodeableConcept(patientBuilder, CodeableConceptBuilder.Tag.Patient_Religion, null);
            BartsCodeableConceptHelper.applyCodeDescTxt(religionCell, CodeValueSet.RELIGION, patientBuilder, CodeableConceptBuilder.Tag.Patient_Religion, csvHelper);*/

            // If we have a deceased date, set that but if not and the patient is deceased just set the deceased flag
            CsvCell deceasedDateTimeCell = parser.getDeceasedDateTime();
            CsvCell deceasedMethodCell = parser.getDeceasedMethodCode();
            if (!BartsCsvHelper.isEmptyOrIsEndOfTime(deceasedDateTimeCell)) {

                //could be in one of two format
                Date dod = BartsCsvHelper.parseDate(deceasedDateTimeCell);
                patientBuilder.setDateOfDeath(dod, deceasedDateTimeCell);

            } else if (!BartsCsvHelper.isEmptyOrIsZero(deceasedMethodCell)) {

                //the deceased method points to a code containing various reasons for death
                //or a simple status of "No" to indicate they're not deceased
                CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.DECEASED_STATUS, deceasedMethodCell);
                String codeDesc = codeRef.getCodeDispTxt();
                if (!codeDesc.equals("No")) {
                    patientBuilder.setDateOfDeathBoolean(true, deceasedMethodCell);
                }

            } else {
                //if updating a record, we may have REMOVED a date of death set incorrectly, so clear the fields on the patient
                patientBuilder.clearDateOfDeath();
            }

        } finally {
            //we don't save the patient here; there are subsequent transforms that work on the patients so we
            //save patients after all of them are done
            csvHelper.getPatientCache().returnPatientBuilder(millenniumPersonIdCell, patientBuilder);
        }

    }

    private static Religion mapReligion(CernerCodeValueRef cvref) throws Exception {
        String s = cvref.getAliasNhsCdAlias();

        switch (s) {
            case "BAPTIST":
                return Religion.BAPTIST;
            case "BUDDHISM":
                return Religion.BUDDHIST;
            case "ROMANCATHOLIC":
                return Religion.ROMAN_CATHOLIC;
            case "HINDU":
                return Religion.HINDU;
            case "JEHOVAHSWITNESS":
                return Religion.JEHOVAHS_WITNESS;
            case "LUTHERAN":
                return Religion.LUTHERAN;
            case "METHODIST":
                return Religion.METHODIST;
            case "ISLAM":
                return Religion.MUSLIM;
            case "SEVENTHDAYADVENTIST":
                return Religion.SEVENTH_DAY_ADVENTIST;
            case "OTHER":
                return Religion.RELIGION_OTHER;
            case "JUDAISMJEWISHHEBREW":
                return Religion.JEWISH;
            case "PRESBYTERIAN":
                return Religion.PRESBYTERIAN;
            case "NAZARENE":
                return Religion.NAZARENE_CHURCH;
            case "LATTERDAYSAINTS":
                return Religion.MORMON;
            case "GREEKORTHODOX":
                return Religion.GREEK_ORTHODOX;
            case "PENTECOSTAL":
                return Religion.PENTECOSTALIST;
            case "CHURCHOFENGLAND":
                return Religion.CHURCH_OF_ENGLAND;
            case "ETHIOPIANORTHODOXRASTAFARIAN":
                return Religion.RASTAFARI;
            case "SIKH":
                return Religion.SIKH;
            case "PLYMOUTHBRETHREN":
                return Religion.PLYMOUTH_BRETHREN;
            case "CHRISTIANCHURCH":
                return Religion.CHRISTIAN;
            case "CHURCHOFGOD":
                return Religion.CHURCH_OF_GOD_OF_PROPHECY;
            case "ASSEMBLIESOFGOD":
                return null;
            case "CHURCHOFCHRIST":
                return null;
            case "EPISCOPAL":
                return null;
            default:
                throw new Exception("Unmapped religion " + s);
        }
    }

    /**
     * the PPALI transformer also creates identifiers for the Patient resource, and sets more information on them (e.g. period)
     * so we can't just create new Identifiers, but should try to match and update
     */
    private static void addOrUpdateIdentifier(PatientBuilder patientBuilder, String value, CsvCell sourceCell, Identifier.IdentifierUse use, String system) {

        //match to an existing identifier for the same system
        Identifier existingIdentifier = null;

        List<Identifier> identifiersForSameSystem = IdentifierBuilder.findExistingIdentifiersForSystem(patientBuilder, system);
        for (Identifier identifier : identifiersForSameSystem) {

            //we get updates to PPATI when another field has changed (e.g. religion), so if the PPALI has
            //already replaced the identifier with one with an ID but our value is the same, then do nothing
            String existingValue = identifier.getValue();
            if (existingValue.equalsIgnoreCase(value)) {
                return;
            }

            //The PPALI transform sets the ID on the Identifiers
            //that it creates, so only match to one that doesn't have an ID set. If the MRN (for example) has been changed
            //there should also be an update to the PPALI file which will remove any unnecessary Identifier we create here
            if (!identifier.hasId()) {
                existingIdentifier = identifier;
                break;
            }
        }

        //create the Identity builder, which will generate a new one if the existing variable is still null
        IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder, existingIdentifier);
        identifierBuilder.setUse(use);
        identifierBuilder.setSystem(system);
        identifierBuilder.setValue(value, sourceCell);
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


    private static NhsNumberVerificationStatus convertNhsNumberVeriticationStatus(String nhsNumberStatus, ParserI parser) throws Exception {

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
                TransformWarnings.log(LOG, parser, "Unmapped NHS number status {}", nhsNumberStatus);
                return null;
        }
    }

    private static MaritalStatus convertMaritalStatus(String statusCode, ParserI parser) throws Exception {
        switch (statusCode) {
            case "DIVORCED":
                return MaritalStatus.DIVORCED;
            case "MARRIED":
                return MaritalStatus.MARRIED;
            case "LGL_SPRTN":
                return MaritalStatus.LEGALLY_SEPARATED;
            case "SINGLE":
                return MaritalStatus.NEVER_MARRIED;
            case "UNKNOWN":
                return null;
            case "WIDOW":
                return MaritalStatus.WIDOWED;
            case "LIFE_PTNR":
                return MaritalStatus.DOMESTIC_PARTNER;
            default:
                TransformWarnings.log(LOG, parser, "Unmapped marital status {}", statusCode);
                return null;
        }
    }
}
