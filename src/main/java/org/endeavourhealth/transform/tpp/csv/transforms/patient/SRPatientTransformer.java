package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.ContactPointBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatient;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        PatientBuilder patientBuilder = csvHelper.getPatientResourceCache().getOrCreatePatientBuilder(rowIdCell, csvHelper);

        CsvCell removeDataCell = parser.getRemovedData();
        if (removeDataCell != null && removeDataCell.getIntAsBoolean()) {
            patientBuilder.setDeletedAudit(removeDataCell);
            csvHelper.getPatientResourceCache().addToPendingDeletes(rowIdCell, patientBuilder);
            return;
        }

        createIdentifier(patientBuilder, csvHelper, rowIdCell, FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_PATIENT_ID, Identifier.IdentifierUse.SECONDARY);

        CsvCell nhsNumberCell = parser.getNHSNumber();
        createIdentifier(patientBuilder, csvHelper, nhsNumberCell, FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER, Identifier.IdentifierUse.OFFICIAL);

        NhsNumberVerificationStatus numberVerificationStatus = null;
        CsvCell spineMatchedCell = parser.getSpineMatched();
        if (spineMatchedCell != null  //need null check because it's not in all versions
                && !spineMatchedCell.isEmpty()
                && !nhsNumberCell.isEmpty()) { //this is only relevant if there is an NHS number
            numberVerificationStatus = mapSpindeMatchedStatus(spineMatchedCell);
        }
        patientBuilder.setNhsNumberVerificationStatus(numberVerificationStatus, spineMatchedCell);

        createName(patientBuilder, parser, csvHelper);

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
        ceatePatientContactEmail(patientBuilder, parser, csvHelper, emailCell);

        //Speaks English
        //The SRPatient CSV record refers to the global mapping file, which supports only three values
        CsvCell speaksEnglishCell = parser.getSpeaksEnglish();
        Boolean speaksEnglishValue = lookUpSpeaksEnglishValue(speaksEnglishCell, csvHelper);
        patientBuilder.setSpeaksEnglish(speaksEnglishValue, speaksEnglishCell);

        //see if there is a ethnicity for patient from pre-transformer (but don't set to null if a new one hasn't been received)
        TppCsvHelper.DateAndCode newEthnicity = csvHelper.findEthnicity(rowIdCell);
        if (newEthnicity != null) {
            EthnicCategory ethnicCategory = EthnicCategory.fromCode(newEthnicity.getCode());
            CsvCell[] additionalSourceCells = newEthnicity.getAdditionalSourceCells();
            patientBuilder.setEthnicity(ethnicCategory, additionalSourceCells);
        }

        //see if there is a marital status for patient from pre-transformer (but don't set to null if a new one hasn't been received)
        TppCsvHelper.DateAndCode newMaritalStatus = csvHelper.findMaritalStatus(rowIdCell);
        if (newMaritalStatus != null) {
            MaritalStatus maritalStatus = MaritalStatus.fromCode(newMaritalStatus.getCode());
            CsvCell[] additionalSourceCells = newMaritalStatus.getAdditionalSourceCells();
            patientBuilder.setMaritalStatus(maritalStatus, additionalSourceCells);
        }

        CsvCell testPatientCell = parser.getTestPatient();
        patientBuilder.setTestPatient(testPatientCell.getBoolean(), testPatientCell);

        //IDOrgVisible to is "here" (the service being transformed), so carry that over to the managing organisation
        CsvCell idOrgVisibleToCell = parser.getIDOrganisationVisibleTo();
        Reference orgReferencePatient = csvHelper.createOrganisationReference(idOrgVisibleToCell);
        if (patientBuilder.isIdMapped()) {
            orgReferencePatient = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReferencePatient, csvHelper);
        }
        patientBuilder.setManagingOrganisation(orgReferencePatient, idOrgVisibleToCell);
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
    private static void ceatePatientContactEmail(PatientBuilder patientBuilder, SRPatient parser, TppCsvHelper csvHelper, CsvCell cell) throws Exception {
        if (!cell.isEmpty()) {

            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setValue(cell.getString(), cell);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME);

            ContactPointBuilder.deDuplicateLastContactPoint(patientBuilder, csvHelper.getDataDate());

        } else {
            ContactPointBuilder.endContactPoints(patientBuilder, csvHelper.getDataDate(), ContactPoint.ContactPointSystem.EMAIL, ContactPoint.ContactPointUse.HOME);
        }

    }

    private static void createName(PatientBuilder patientBuilder, SRPatient parser, TppCsvHelper csvHelper) throws Exception {

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

            NameBuilder.deDuplicateLastName(patientBuilder, csvHelper.getDataDate());

        } else {
            //if no name (for some reason), just end any existing ones in the resource
            NameBuilder.endNames(patientBuilder, csvHelper.getDataDate(), HumanName.NameUse.OFFICIAL);
        }
    }

    private static void createIdentifier(PatientBuilder patientBuilder, TppCsvHelper csvHelper, CsvCell cell, String system, Identifier.IdentifierUse use) throws Exception {

        if (!cell.isEmpty()) {
            IdentifierBuilder identifierBuilderTpp = new IdentifierBuilder(patientBuilder);
            identifierBuilderTpp.setSystem(system);
            identifierBuilderTpp.setUse(use);
            identifierBuilderTpp.setValue(cell.getString(), cell);

            IdentifierBuilder.deDuplicateLastIdentifier(patientBuilder, csvHelper.getDataDate());

        } else {
            IdentifierBuilder.endIdentifiers(patientBuilder, csvHelper.getDataDate(), system, use);
        }
    }

    private static NhsNumberVerificationStatus mapSpindeMatchedStatus(CsvCell spineMatched) {
        String s = spineMatched.getString();
        if (Boolean.parseBoolean(s)) {
            return NhsNumberVerificationStatus.PRESENT_AND_VERIFIED;

        } else {
            return NhsNumberVerificationStatus.PRESENT_BUT_NOT_TRACED;
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

}
