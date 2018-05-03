package org.endeavourhealth.transform.tpp.csv.transforms.Patient;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.PatientResourceCache;
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

        AbstractCsvParser parser = parsers.get(SRPatientTransformer.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRPatient)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(SRPatient parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();
        CsvCell nhsNumberCell = parser.getNHSNumber();
        if ((rowIdCell.isEmpty()) || (!StringUtils.isNumeric(rowIdCell.getString())) ) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifer: {} in file : {}",rowIdCell.getString(), parser.getFilePath());
            return;
        }

        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(rowIdCell, csvHelper,fhirResourceFiler);

        CsvCell removeDataCell = parser.getRemovedData();
        if (removeDataCell.getIntAsBoolean()) {
            if (PatientResourceCache.patientInCache(rowIdCell)) {
                PatientResourceCache.removePatientByRowId(rowIdCell, fhirResourceFiler,parser);
            }
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), patientBuilder);
            return;
        }

        if (!nhsNumberCell.isEmpty()) {
            String nhsNumber = nhsNumberCell.getString();
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);
            identifierBuilder.setValue(nhsNumber, nhsNumberCell);
            } else {
            TransformWarnings.log(LOG, parser, "No NHS number found record id: {}, file: {}", parser.getRowIdentifier().getString(), parser.getFilePath());
        }

        //Construct the name from individual fields
        CsvCell firstNameCell = parser.getFirstName();
        CsvCell surnameCell = parser.getSurname();
        CsvCell middleNamesCell = parser.getMiddleNames();
        CsvCell titleCell = parser.getTitle();

        NameBuilder nameBuilder = new NameBuilder(patientBuilder);
        nameBuilder.setId(nhsNumberCell.getString(), nhsNumberCell);
        if (!titleCell.isEmpty()) {
            nameBuilder.addPrefix(titleCell.getString(), titleCell);
        }
        nameBuilder.addGiven(firstNameCell.getString(), firstNameCell);
        if (!middleNamesCell.isEmpty()) {
            nameBuilder.addGiven(middleNamesCell.getString(), middleNamesCell);
        }
        nameBuilder.addFamily(surnameCell.getString(), surnameCell);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);


        CsvCell dobCell = parser.getDateBirth();
        if (!dobCell.isEmpty()) {
            patientBuilder.setDateOfBirth(dobCell.getDate(), dobCell);
        }
        CsvCell dateDeathCell = parser.getDateDeath();

        if (!dateDeathCell.isEmpty()) {
            patientBuilder.setDateOfDeath(dateDeathCell.getDate(), dateDeathCell);
        }

        CsvCell genderCell = parser.getGender();
        if (!genderCell.isEmpty()) {
            if (genderCell.getString().equalsIgnoreCase("m")) {
                patientBuilder.setGender(Enumerations.AdministrativeGender.MALE, genderCell);
            } else if (genderCell.getString().equalsIgnoreCase("f")) {
                patientBuilder.setGender(Enumerations.AdministrativeGender.FEMALE);
            } else {
                TransformWarnings.log(LOG, parser, "Unknown gender code :{} in file {}", genderCell.getString(), parser.getFilePath());
            }
        }


        CsvCell emailCell = parser.getEmailAddress();

        if (!emailCell.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setValue(emailCell.getString(), emailCell);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL,emailCell);

        }
        CsvCell spineMatched = parser.getSpineMatched();
        if (!spineMatched.isEmpty()) {
            NhsNumberVerificationStatus numberVerificationStatus;
            if (spineMatched.getString().equalsIgnoreCase("true")) {
                numberVerificationStatus = NhsNumberVerificationStatus.PRESENT_AND_VERIFIED;
                patientBuilder.setNhsNumberVerificationStatus(numberVerificationStatus, spineMatched);
            } else if (spineMatched.getString().equalsIgnoreCase("false")) {
                numberVerificationStatus = NhsNumberVerificationStatus.PRESENT_BUT_NOT_TRACED;
                patientBuilder.setNhsNumberVerificationStatus(numberVerificationStatus, spineMatched);
            } else {
                TransformWarnings.log(LOG, parser, "NHS number verification status unknown : {} for Id :{} in file:{}",
                        spineMatched.getString(), parser.getRowIdentifier().toString(), parser.getFilePath());
            }
        }

        // Speaks English
        // In TPP we just have a "speaks english" column with these values so do what we can
        //        "54771","Unknown"
        //        "54770","Yes" - set language as English
        //        "54772","No"  - set interpreter required to true

        CsvCell speaksEnglishCell = parser.getSpeaksEnglish();
        if (!speaksEnglishCell.isEmpty()) {
            CodeableConcept englishSpoken = patientBuilder.createNewCodeableConcept(PatientBuilder.TAG_CODEABLE_CONCEPT_LANGUAGE);

                switch (speaksEnglishCell.getString().toLowerCase()) {
                    case "54770" :
                        englishSpoken.setText("yes");
                        break;
                    case "54772" :
                        englishSpoken.setText("no");
                        break;
                    case "54771":
                        // removed below as unknown is TPP default but still a valid value
                        // englishSpoken.setText("unknown");
                        break;
                    default:
                        englishSpoken.setText("");
                        TransformWarnings.log(LOG, parser, "Unrecognized EnglishSpoken value {} for Id: {} in file {}",
                                speaksEnglishCell.getString(),parser.getRowIdentifier().getString(), parser.getFilePath());
            }

        }
    }

}
