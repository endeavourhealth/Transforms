package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.PatientResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientRelationship;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SRPatientRelationshipTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRPatientRelationshipTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRPatientRelationship.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRPatientRelationship) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(SRPatientRelationship parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {
        CsvCell rowIdCell = parser.getRowIdentifier();
        if ((rowIdCell.isEmpty()) || (!StringUtils.isNumeric(rowIdCell.getString()))) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifier: {} in file : {}", rowIdCell.getString(), parser.getFilePath());
            return;
        }
        CsvCell IdPatientCell = parser.getIDPatient();
        PatientBuilder patientBuilder = PatientResourceCache.getOrCreatePatientBuilder(IdPatientCell, csvHelper, fhirResourceFiler);

        PatientContactBuilder contactBuilder = new PatientContactBuilder(patientBuilder);
        contactBuilder.setId(rowIdCell.getString(), rowIdCell);

        CsvCell removeDataCell = parser.getRemovedData();
        if ((removeDataCell != null) && !removeDataCell.isEmpty() && removeDataCell.getIntAsBoolean()) {
            List<Patient.ContactComponent> contacts = patientBuilder.getPatientContactComponents();
            for (Patient.ContactComponent cc: contacts) {
                if (cc.getId().equals(rowIdCell.getString())) {
                    patientBuilder.removePatientContactComponent(cc);
                }
            }
            return;
        }

        if (IdPatientCell.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }
        CsvCell relationshipTypeCell = parser.getRelationshipType();
        if (!relationshipTypeCell.isEmpty()) {
            String rel = csvHelper.tppRelationtoFhir(relationshipTypeCell.getString());
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(contactBuilder, CodeableConceptBuilder.Tag.Patient_Contact_Relationship);
            codeableConceptBuilder.setText(rel);

        }

        CsvCell relationshipWithNameCell = parser.getRelationshipWithName();
        if (!relationshipWithNameCell.isEmpty()) {
            HumanName name = new HumanName();
            if (!parser.getIDPatientRelationshipWith().isEmpty()) {
                PatientBuilder relationshipBuilder = PatientResourceCache.getOrCreatePatientBuilder(
                        parser.getIDPatientRelationshipWith(), csvHelper, fhirResourceFiler);
                if (relationshipBuilder.getNames().size() > 0) {
                    name = relationshipBuilder.getNames().get(0);
                }
            }
            if (!name.hasText()) {
                name.setText(relationshipWithNameCell.getString());
            }
            contactBuilder.addContactName(name);
        }
        AddressBuilder addressBuilder = new AddressBuilder(contactBuilder);
        CsvCell nameOfBuildingCell = parser.getRelationshipWithHouseName();
        if (!nameOfBuildingCell.isEmpty()) {
            addressBuilder.addLine(nameOfBuildingCell.getString(), nameOfBuildingCell);
        }
        CsvCell numberOfBuildingCell = parser.getRelationshipWithHouseNumber();
        CsvCell nameOfRoadCell = parser.getRelationshipWithRoad();
        StringBuilder next = new StringBuilder();
        // Some addresses have a house name with or without a street number or road name
        // Try to handle combinations
        if (!numberOfBuildingCell.isEmpty()) {
            next.append(numberOfBuildingCell.getString());
        }
        if (!nameOfRoadCell.isEmpty()) {
            next.append(" ");
            next.append(nameOfRoadCell.getString());
        }
        if (next.length() > 0) {
            addressBuilder.addLine(next.toString());
        }
        CsvCell nameOfLocalityCell = parser.getRelationshipWithLocality();
        if (!nameOfLocalityCell.isEmpty()) {
            addressBuilder.addLine(nameOfLocalityCell.getString(), nameOfLocalityCell);
        }
        CsvCell nameOfTownCell = parser.getRelationshipWithPostTown();
        if (!nameOfTownCell.isEmpty()) {
            addressBuilder.addLine(nameOfTownCell.getString(), nameOfTownCell);
        }
        CsvCell nameOfCountyCell = parser.getRelationshipWithCounty();
        if (!nameOfCountyCell.isEmpty()) {
            addressBuilder.addLine(nameOfCountyCell.getString(), nameOfCountyCell);
        }
        CsvCell fullPostCodeCell = parser.getRelationshipWithPostCode();
        if (!fullPostCodeCell.isEmpty()) {
            addressBuilder.addLine(fullPostCodeCell.getString(), fullPostCodeCell);
        }

        CsvCell relWithTelephone = parser.getRelationshipWithTelephone();
        if (!(relWithTelephone).isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME, relWithTelephone);
            contactPointBuilder.setValue(relWithTelephone.getString(), relWithTelephone);
        }
        CsvCell relWithWorkTelephone = parser.getRelationshipWithWorkTelephone();
        if (!(relWithWorkTelephone).isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK, relWithWorkTelephone);
            contactPointBuilder.setValue(relWithWorkTelephone.getString(), relWithWorkTelephone);
        }
        CsvCell relWithMobileTelephone = parser.getRelationshipWithMobileTelephone();
        if (!(relWithWorkTelephone).isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.MOBILE, relWithMobileTelephone);
            contactPointBuilder.setValue(relWithMobileTelephone.getString(), relWithMobileTelephone);
        }
        CsvCell relationshipWithFax = parser.getRelationshipWithFax();
        if (!(relationshipWithFax).isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.FAX);
            contactPointBuilder.setValue(relationshipWithFax.getString(), relationshipWithFax);
        }
        CsvCell relationshipWithEmail = parser.getRelationshipWithEmailAddress();
        if (!(relationshipWithEmail).isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL);
            contactPointBuilder.setValue(relationshipWithEmail.getString(), relationshipWithEmail);
        }
        // boolean mapIds = !patientBuilder.isIdMapped();
        //fhirResourceFiler.savePatientResource(parser.getCurrentState(),mapIds,patientBuilder);
        // Filing done by cache
    }
}
