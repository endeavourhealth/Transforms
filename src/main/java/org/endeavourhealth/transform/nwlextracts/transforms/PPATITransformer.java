package org.endeavourhealth.transform.nwlextracts.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
import org.endeavourhealth.transform.nwlextracts.CodeValueSet;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCodeableConceptHelper;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.endeavourhealth.transform.nwlextracts.helpers.NwlExtractsHelper;
import org.endeavourhealth.transform.nwlextracts.schema.PPATI;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Enumerations;
import org.hl7.fhir.instance.model.Extension;
import org.hl7.fhir.instance.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PPATITransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPATITransformer.class);


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        NwlExtractsHelper nwlextractsHelper = new NwlExtractsHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(),
                fhirResourceFiler.getExchangeId(),  null);
        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {

                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
                    continue;
                }

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                createPatient((PPATI) parser, fhirResourceFiler, csvHelper,nwlextractsHelper,sourceSystem);
            }
        }
    }

    public static void createPatient(PPATI parser, FhirResourceFiler fhirResourceFiler, NwlExtractsCsvHelper csvHelper,NwlExtractsHelper nwlextractsHelper, String sourceSystem) throws Exception {

        //this transform always UPDATES resources when possible, so we use the patient cache to retrieve from the DB

        CsvCell millenniumPersonIdCell = parser.getMillenniumPersonId();
        CsvCell mrnCell = parser.getLocalPatientId();


        ArrayList<String> patientIdentifiers = new ArrayList<String>();
        patientIdentifiers.add(0,millenniumPersonIdCell.getString());
        patientIdentifiers.add(1,mrnCell.getString());

        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(millenniumPersonIdCell,sourceSystem);
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

            if (!mrnCell.isEmpty()) {
                addOrUpdateIdentifier(patientBuilder, mrnCell.getString(), mrnCell, Identifier.IdentifierUse.SECONDARY,  "https://fhir.hl7.org.uk/Id/"+ sourceSystem.toLowerCase());
            }

            CsvCell nhsNumberCell = parser.getNhsNumber();
            if (!nhsNumberCell.isEmpty()) {
                String nhsNumber = nhsNumberCell.getString();
                nhsNumber = nhsNumber.replace("-", ""); //Cerner NHS numbers are tokenised with hyphens, so remove
                nhsNumber = nhsNumber.replaceAll("\\s+", ""); //Remove the spaces between the numbers

                addOrUpdateIdentifier(patientBuilder, nhsNumber, nhsNumberCell, Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);
            }

            CsvCell nhsNumberStatusCell = parser.getNhsNumberStatus();
            if (!NwlExtractsCsvHelper.isEmptyOrIsZero(nhsNumberStatusCell)) {

                CsvCell cernerDescCell = NwlExtractsCodeableConceptHelper.getCellDesc(csvHelper, CodeValueSet.NHS_NUMBER_STATUS, nhsNumberStatusCell);
                if (cernerDescCell == null) {
                    TransformWarnings.log(LOG, parser, "Cerner code Nhs Number status {} for PPATI {} not found", nhsNumberStatusCell, millenniumPersonIdCell);

                } else {
                    NhsNumberVerificationStatus verificationStatus = convertNhsNumberVerificationStatus(cernerDescCell.getString(), parser);
                    patientBuilder.setNhsNumberVerificationStatus(verificationStatus, nhsNumberStatusCell, cernerDescCell);
                }

            } else {
                //we may be updating a patient, so make sure to remove if not set
                patientBuilder.setNhsNumberVerificationStatus(null);
            }

            CsvCell dateOfBirthCell = parser.getDateOfBirth();
            if (!NwlExtractsCsvHelper.isEmptyOrIsEndOfTime(dateOfBirthCell)) {
                //we need to handle multiple formats, so attempt to apply both formats here
                Date dob = NwlExtractsCsvHelper.parseDate(dateOfBirthCell);
                patientBuilder.setDateOfBirth(dob, dateOfBirthCell);

            } else {

                TransformWarnings.log(LOG, parser, "Cerner Date of Birth {} for the Patient ID {} in PPATI not found", dateOfBirthCell, millenniumPersonIdCell);
                // SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, "Date of Birth not found for the Patient ID in PPATI " + millenniumPersonIdCell+ "for ODS Code" +NwlExtractsCsvToFhirTransformer.PRIMARY_ORG_ODS_CODE);
                //we may be updating an existing patient
                patientBuilder.setDateOfBirth(null);
            }

            CsvCell genderCell = parser.getGenderCode();
            if (!NwlExtractsCsvHelper.isEmptyOrIsZero(genderCell)) {
                CsvCell genderMeaningCell = NwlExtractsCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.GENDER, genderCell);
                if (genderMeaningCell == null) {
                    TransformWarnings.log(LOG, parser, "Cerner gender code {} for the Patient ID  {} in PPATI not found", genderCell, millenniumPersonIdCell);
                    //  SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, "Gender Code not found for the Patient ID in PPATI " + millenniumPersonIdCell+ "for ODS Code" +NwlExtractsCsvToFhirTransformer.PRIMARY_ORG_ODS_CODE);

                } else {
                    Enumerations.AdministrativeGender gender = SexConverter.convertCernerSexToFhir(genderMeaningCell.getString());
                    patientBuilder.setGender(gender, genderCell, genderMeaningCell);
                }

            } else {
                //if updating a record then clear the gender if the field is empty
                patientBuilder.setGender(null);
            }

            CsvCell maritalStatusCode = parser.getMaritalStatusCode();
            if (!NwlExtractsCsvHelper.isEmptyOrIsZero(maritalStatusCode)) {

                CsvCell maritalMeaningCell = NwlExtractsCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.MARITAL_STATUS, maritalStatusCode);
                if (maritalMeaningCell == null) {
                    TransformWarnings.log(LOG, parser, "Cerner marital status {} for the Patient ID {} in PPATI not found", maritalStatusCode, millenniumPersonIdCell);

                } else {
                    MaritalStatus maritalStatus = convertMaritalStatus(maritalMeaningCell.getString(), parser);
                    patientBuilder.setMaritalStatus(maritalStatus, maritalStatusCode, maritalMeaningCell);
                }
            } else {
                //if updating a record, make sure to clear the field in this case
                patientBuilder.setMaritalStatus(null);
            }

            CsvCell ethnicityCode = parser.getEthnicGroupCode();
            if (!NwlExtractsCsvHelper.isEmptyOrIsZero(ethnicityCode)) {

                    String ethnicity = ethnicityCode.getString();
                    MapResponse valueResponse = NwlExtractsHelper.retrieveIMLookupMap(sourceSystem, "ethnicity", ethnicity);
                    if (valueResponse.getWasCreated()) {
                        NwlExtractsHelper.raiseMissingAlert(ethnicity, "ethnicity", sourceSystem, nwlextractsHelper.getExchangeId().toString(), NwlExtractsHelper.getPatientGUID(patientIdentifiers), nwlextractsHelper);
                    }
                    ExtensionConverter.removeExtension(patientBuilder.getResource(), FhirExtensionUri.PATIENT_ETHNICITY);
                    CodeableConcept ccValue = new CodeableConcept();
                    ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                            .setSystem(valueResponse.getConcept().getScheme()).setDisplay(ethnicity);

                    Extension extension = ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_ETHNICITY, ccValue);
                    patientBuilder.getResource().addExtension(extension);
            } else {
                //if this field is empty we should clear the value from the patient
                patientBuilder.setEthnicity(null);
            }

            // If we have a deceased date, set that but if not and the patient is deceased just set the deceased flag
            CsvCell deceasedDateTimeCell = parser.getDeceasedDateTime();
            CsvCell deceasedMethodCell = parser.getDeceasedMethodCode();
            if (!NwlExtractsCsvHelper.isEmptyOrIsEndOfTime(deceasedDateTimeCell)) {

                //could be in one of two format
                Date dod = NwlExtractsCsvHelper.parseDate(deceasedDateTimeCell);
                patientBuilder.setDateOfDeath(dod, deceasedDateTimeCell);

            } else if (!NwlExtractsCsvHelper.isEmptyOrIsZero(deceasedMethodCell)) {

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

    private static NhsNumberVerificationStatus convertNhsNumberVerificationStatus(String nhsNumberStatus, ParserI parser) throws Exception {

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
