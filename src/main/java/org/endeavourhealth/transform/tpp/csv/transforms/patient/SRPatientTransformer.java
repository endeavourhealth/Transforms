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

import java.util.Iterator;
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
        CsvCell nhsNumberCell = parser.getNHSNumber();

        PatientBuilder patientBuilder = csvHelper.getPatientResourceCache().getOrCreatePatientBuilder(rowIdCell, csvHelper);

        CsvCell removeDataCell = parser.getRemovedData();
        if (removeDataCell != null && removeDataCell.getIntAsBoolean()) {
            patientBuilder.setDeletedAudit(removeDataCell);
            csvHelper.getPatientResourceCache().addToPendingDeletes(rowIdCell, patientBuilder);
            return;
        }

        //remove existing PatientId identifier to prevent any duplication
        IdentifierBuilder.removeExistingIdentifiersForSystem(patientBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_PATIENT_ID);

        IdentifierBuilder identifierBuilderTpp = new IdentifierBuilder(patientBuilder);
        identifierBuilderTpp.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_PATIENT_ID);
        identifierBuilderTpp.setUse(Identifier.IdentifierUse.SECONDARY);
        identifierBuilderTpp.setValue(rowIdCell.getString(), rowIdCell);

        if (!nhsNumberCell.isEmpty()) {

            //remove existing NHS number identifier to prevent any duplication
            IdentifierBuilder.removeExistingIdentifiersForSystem(patientBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);

            String nhsNumber = nhsNumberCell.getString();
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setValue(nhsNumber, nhsNumberCell);

            //this is only relevant if there is an NHS number
            CsvCell spineMatched = parser.getSpineMatched();
            if (spineMatched != null && !spineMatched.isEmpty()) { //need null check because it's not in all versions
                NhsNumberVerificationStatus numberVerificationStatus = mapSpindeMatchedStatus(spineMatched);
                patientBuilder.setNhsNumberVerificationStatus(numberVerificationStatus, spineMatched);
            }
        }

        //Construct the name from individual fields
        CsvCell firstNameCell = parser.getFirstName();
        CsvCell surnameCell = parser.getSurname();
        CsvCell middleNamesCell = parser.getMiddleNames();
        CsvCell titleCell = parser.getTitle();

        //We don't want a new *OFFICIAL* HumanName added for every local id.
        // Delete existing Official name and replace
        for (Iterator<HumanName> iterator = patientBuilder.getNames().iterator(); iterator.hasNext(); ) {
        // for (HumanName nom : patientBuilder.getNames()) {
            HumanName nom = iterator.next();
            if (nom.getUse().equals(HumanName.NameUse.OFFICIAL)) {
                patientBuilder.getNames().remove(nom);
                break;
            }
        }
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

        CsvCell dobCell = parser.getDateBirth();
        if (!dobCell.isEmpty()) {
            //SystmOne captures time of birth too, so don't lose this by treating just as a date
            patientBuilder.setDateOfBirth(dobCell.getDateTime(), dobCell);
            //patientBuilder.setDateOfBirth(dobCell.getDate(), dobCell);
        }
        CsvCell dateDeathCell = parser.getDateDeath();

        if (!dateDeathCell.isEmpty()) {
            patientBuilder.setDateOfDeath(dateDeathCell.getDate(), dateDeathCell);
        }

        CsvCell genderCell = parser.getGender();
        if (!genderCell.isEmpty()) {
            Enumerations.AdministrativeGender gender = mapGender(genderCell);
            patientBuilder.setGender(gender, genderCell);
        }

        CsvCell emailCell = parser.getEmailAddress();
        if (!emailCell.isEmpty()) {
            // Remove any existing email.
            ContactPointBuilder.removeExistingContactPointsBySystem(patientBuilder, ContactPoint.ContactPointSystem.EMAIL);
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setValue(emailCell.getString(), emailCell);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL);
        }

        //Speaks English
        //The SRPatient CSV record refers to the global mapping file, which supports only three values
        CsvCell speaksEnglishCell = parser.getSpeaksEnglish();
        if (!speaksEnglishCell.isEmpty()) {

            TppMappingRef mapping = csvHelper.lookUpTppMappingRef(speaksEnglishCell);
            if (mapping != null) {
                String term = mapping.getMappedTerm();
                if (term.equals("Unknown")) {
                    patientBuilder.setSpeaksEnglish(null, speaksEnglishCell);

                } else if (term.equals("Yes")) {
                    patientBuilder.setSpeaksEnglish(Boolean.TRUE, speaksEnglishCell);

                } else if (term.equals("No")) {
                    patientBuilder.setSpeaksEnglish(Boolean.FALSE, speaksEnglishCell);

                } else {
                    throw new TransformException("Unexpected english speaks value [" + term + "]");
                }
            }
        }

        //see if there is a marital status for patient from pre-transformer
        CodeableConcept fhirMartialStatus = csvHelper.findMaritalStatus(rowIdCell);
        if (fhirMartialStatus != null) {
            String maritalStatusCode = CodeableConceptHelper.getFirstCoding(fhirMartialStatus).getCode();
            if (!Strings.isNullOrEmpty(maritalStatusCode)) {
                patientBuilder.setMaritalStatus(MaritalStatus.fromCode(maritalStatusCode));
            }
        }

        //see if there is a ethnicity for patient from pre-transformer
        CodeableConcept fhirEthnicity = csvHelper.findEthnicity(rowIdCell);
        if (fhirEthnicity != null) {
            String ethnicityCode = CodeableConceptHelper.getFirstCoding(fhirEthnicity).getCode();
            if (!Strings.isNullOrEmpty(ethnicityCode)) {
                patientBuilder.setEthnicity(EthnicCategory.fromCode(ethnicityCode));
            }
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
