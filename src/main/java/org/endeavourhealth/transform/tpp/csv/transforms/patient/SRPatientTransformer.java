package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.helpers.TppMappingHelper;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatient;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SRPatientTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRPatientTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRPatient.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRPatient) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(SRPatient parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();
        PatientBuilder patientBuilder = csvHelper.getPatientResourceCache().borrowPatientBuilder(rowIdCell, csvHelper, fhirResourceFiler, true);

        //if deleted, delete all data for this patient
        CsvCell removeDataCell = parser.getRemovedData();
        if (removeDataCell != null && removeDataCell.getIntAsBoolean()) {
            deleteEntirePatientRecord(fhirResourceFiler, parser);
            return;
        }

        try {
            createIdentifier(patientBuilder, fhirResourceFiler, rowIdCell, FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_PATIENT_ID, Identifier.IdentifierUse.SECONDARY);

            CsvCell nhsNumberCell = parser.getNHSNumber();
            createIdentifier(patientBuilder, fhirResourceFiler, nhsNumberCell, FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER, Identifier.IdentifierUse.OFFICIAL);

            NhsNumberVerificationStatus numberVerificationStatus = null;
            CsvCell spineMatchedCell = parser.getSpineMatched();
            if (spineMatchedCell != null  //need null check because it's not in all versions
                    && !spineMatchedCell.isEmpty()
                    && !nhsNumberCell.isEmpty()) { //this is only relevant if there is an NHS number
                numberVerificationStatus = mapSpindeMatchedStatus(spineMatchedCell);
            }
            patientBuilder.setNhsNumberVerificationStatus(numberVerificationStatus, spineMatchedCell);

            createName(patientBuilder, parser, fhirResourceFiler);

            CsvCell dobCell = parser.getDateBirth();
            if (!dobCell.isEmpty()) {
                //SystmOne captures time of birth too, so don't lose this by treating just as a date
                patientBuilder.setDateOfBirth(dobCell.getDateTime(), dobCell);
                //patientBuilder.setDateOfBirth(dobCell.getDate(), dobCell);
            } else {
                patientBuilder.setDateOfBirth(null, dobCell);
            }

            CsvCell dateDeathCell = parser.getDateDeath();
            if (!dateDeathCell.isEmpty()) {
                patientBuilder.setDateOfDeath(dateDeathCell.getDate(), dateDeathCell);
            } else {
                patientBuilder.clearDateOfDeath();
            }

            CsvCell genderCell = parser.getGender();
            if (!genderCell.isEmpty()) {
                Enumerations.AdministrativeGender gender = mapGender(genderCell);
                patientBuilder.setGender(gender, genderCell);
            } else {
                patientBuilder.setGender(null, genderCell);
            }

            CsvCell emailCell = parser.getEmailAddress();
            ceatePatientContactEmail(patientBuilder, parser, fhirResourceFiler, emailCell);

            //Speaks English
            //The SRPatient CSV record refers to the global mapping file, which supports only three values
            CsvCell speaksEnglishCell = parser.getSpeaksEnglish();
            Boolean speaksEnglishValue = lookUpSpeaksEnglishValue(speaksEnglishCell, csvHelper);
            patientBuilder.setSpeaksEnglish(speaksEnglishValue, speaksEnglishCell);

            //see if there is a ethnicity for patient from pre-transformer (but don't set to null if a new one hasn't been received)
            TppCsvHelper.DateAndCode newEthnicity = csvHelper.findEthnicity(rowIdCell);
            TppMappingHelper.applyNewEthnicity(newEthnicity, patientBuilder);

            //see if there is a marital status for patient from pre-transformer (but don't set to null if a new one hasn't been received)
            TppCsvHelper.DateAndCode newMaritalStatus = csvHelper.findMaritalStatus(rowIdCell);
            TppMappingHelper.applyNewMaritalStatus(newMaritalStatus, patientBuilder);

            CsvCell testPatientCell = parser.getTestPatient();
            patientBuilder.setTestPatient(testPatientCell.getBoolean(), testPatientCell);

            //IDOrgVisible to is "here" (the service being transformed), so carry that over to the managing organisation
            CsvCell idOrgVisibleToCell = parser.getIDOrganisationVisibleTo();
            Reference orgReferencePatient = csvHelper.createOrganisationReference(idOrgVisibleToCell);
            if (patientBuilder.isIdMapped()) {
                orgReferencePatient = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReferencePatient, csvHelper);
            }
            patientBuilder.setManagingOrganisation(orgReferencePatient, idOrgVisibleToCell);

        } finally {
            csvHelper.getPatientResourceCache().returnPatientBuilder(rowIdCell, patientBuilder);
        }
    }

    private static Boolean lookUpSpeaksEnglishValue(CsvCell speaksEnglishCell, TppCsvHelper csvHelper) throws Exception {
        if (!speaksEnglishCell.isEmpty()) {
            TppMappingRef mapping = csvHelper.lookUpTppMappingRef(speaksEnglishCell);
            String term = mapping.getMappedTerm();
            if (term.equals("Unknown")) {
                return null;

            } else if (term.equals("Yes")) {
                return Boolean.TRUE;

            } else if (term.equals("No")) {
                return Boolean.FALSE;

            } else {
                throw new TransformException("Unexpected english speaks value [" + term + "]");
            }

        } else {
            return null;
        }
    }

    /**
     * adds the email address contact (all other contacts e.g. telephone numbers) are done in SRPatientContactDetailsTransformer
     */
    private static void ceatePatientContactEmail(PatientBuilder patientBuilder, SRPatient parser, FhirResourceFiler fhirResourceFiler, CsvCell cell) throws Exception {
        if (!cell.isEmpty()) {

            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setValue(cell.getString(), cell);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME);

            ContactPointBuilder.deDuplicateLastContactPoint(patientBuilder, fhirResourceFiler.getDataDate());

        } else {
            ContactPointBuilder.endContactPoints(patientBuilder, fhirResourceFiler.getDataDate(), ContactPoint.ContactPointSystem.EMAIL, ContactPoint.ContactPointUse.HOME);
        }

    }

    private static void createName(PatientBuilder patientBuilder, SRPatient parser, FhirResourceFiler fhirResourceFiler) throws Exception {

        //Construct the name from individual fields
        CsvCell firstNameCell = parser.getFirstName();
        CsvCell surnameCell = parser.getSurname();
        CsvCell middleNamesCell = parser.getMiddleNames();
        CsvCell titleCell = parser.getTitle();

        if (!firstNameCell.isEmpty()
                || !surnameCell.isEmpty()
                || !middleNamesCell.isEmpty()
                || !titleCell.isEmpty()) {

            NameBuilder nameBuilder = new NameBuilder(patientBuilder);
            nameBuilder.setUse(HumanName.NameUse.OFFICIAL);

            if (!titleCell.isEmpty()) {
                nameBuilder.addPrefix(titleCell.getString(), titleCell);
            }
            if (!firstNameCell.isEmpty()) {
                nameBuilder.addGiven(firstNameCell.getString(), firstNameCell);
            }
            if (!middleNamesCell.isEmpty()) {
                nameBuilder.addGiven(middleNamesCell.getString(), middleNamesCell);
            }
            if (!surnameCell.isEmpty()) {
                nameBuilder.addFamily(surnameCell.getString(), surnameCell);
            }

            NameBuilder.deDuplicateLastName(patientBuilder, fhirResourceFiler.getDataDate());

        } else {
            //if no name (for some reason), just end any existing ones in the resource
            NameBuilder.endNames(patientBuilder, fhirResourceFiler.getDataDate(), HumanName.NameUse.OFFICIAL);
        }
    }

    private static void createIdentifier(PatientBuilder patientBuilder, FhirResourceFiler fhirResourceFiler, CsvCell cell, String system, Identifier.IdentifierUse use) throws Exception {

        if (!cell.isEmpty()) {
            IdentifierBuilder identifierBuilderTpp = new IdentifierBuilder(patientBuilder);
            identifierBuilderTpp.setSystem(system);
            identifierBuilderTpp.setUse(use);
            identifierBuilderTpp.setValue(cell.getString(), cell);

            IdentifierBuilder.deDuplicateLastIdentifier(patientBuilder, fhirResourceFiler.getDataDate());

        } else {
            IdentifierBuilder.endIdentifiers(patientBuilder, fhirResourceFiler.getDataDate(), system, use);
        }
    }

    private static NhsNumberVerificationStatus mapSpindeMatchedStatus(CsvCell spineMatched) {
        String s = spineMatched.getString();
        if (Boolean.parseBoolean(s)) {
            return NhsNumberVerificationStatus.PRESENT_AND_VERIFIED;

        } else {
            //not enough info to make this choice - just leave null
            return null;
            //return NhsNumberVerificationStatus.PRESENT_BUT_NOT_TRACED;
        }
    }

    private static Enumerations.AdministrativeGender mapGender(CsvCell genderCell) throws TransformException {
        String s = genderCell.getString();

        if (s.equalsIgnoreCase("m")) {
            return Enumerations.AdministrativeGender.MALE;

        } else if (s.equalsIgnoreCase("f")) {
            return Enumerations.AdministrativeGender.FEMALE;

        } else if (s.equalsIgnoreCase("i")) {
            return Enumerations.AdministrativeGender.OTHER;

        } else if (s.equalsIgnoreCase("u")) {
            return Enumerations.AdministrativeGender.UNKNOWN;

        } else {
            throw new TransformException("Unsupported gender " + s);
        }

    }

    /**
     * deletes all FHIR resources associated with this patient record
     */
    private static void deleteEntirePatientRecord(FhirResourceFiler fhirResourceFiler,
                                                  SRPatient parser) throws Exception {

        CsvCell patientIdCell = parser.getRowIdentifier();
        CsvCell deletedCell = parser.getRemovedData();
        CsvCurrentState currentState = parser.getCurrentState();

        String sourceId = patientIdCell.getString();

        PatientDeleteHelper.deleteAllResourcesForPatient(sourceId, fhirResourceFiler, currentState, deletedCell);
    }

}
