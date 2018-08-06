package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.NameConverter;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientRelationship;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.HumanName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRPatientRelationshipTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRPatientRelationshipTransformer.class);

    public static final String RELATIONSHIP_ID_TO_PATIENT_ID = "RelationshipIdToPatientId";

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

        CsvCell removeDataCell = parser.getRemovedData();
        if (removeDataCell != null && removeDataCell.getIntAsBoolean()) {

            //if removed we won't have a patient ID, so need to look it up
            String patientId = csvHelper.getInternalId(RELATIONSHIP_ID_TO_PATIENT_ID, rowIdCell.getString());
            if (!Strings.isNullOrEmpty(patientId)) {
                CsvCell dummyPatientCell = CsvCell.factoryDummyWrapper(patientId);

                PatientBuilder patientBuilder = csvHelper.getPatientResourceCache().getOrCreatePatientBuilder(dummyPatientCell, csvHelper);
                if (patientBuilder != null) {
                    PatientContactBuilder.removeExistingContactPointById(patientBuilder, rowIdCell.getString());
                }
            }
            return;
        }

        CsvCell patientIdCell = parser.getIDPatient();
        PatientBuilder patientBuilder = csvHelper.getPatientResourceCache().getOrCreatePatientBuilder(patientIdCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        //make sure to remove any existing instance from the patient first
        PatientContactBuilder.removeExistingContactPointById(patientBuilder, rowIdCell.getString());

        PatientContactBuilder contactBuilder = new PatientContactBuilder(patientBuilder);
        contactBuilder.setId(rowIdCell.getString(), rowIdCell);

        CsvCell relationshipTypeCell = parser.getRelationshipType();
        if (!relationshipTypeCell.isEmpty()) {
            String rel = csvHelper.tppRelationtoFhir(relationshipTypeCell.getString());
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(contactBuilder, CodeableConceptBuilder.Tag.Patient_Contact_Relationship);
            codeableConceptBuilder.setText(rel);
        }

        CsvCell relationshipWithNameCell = parser.getRelationshipWithName();
        if (!relationshipWithNameCell.isEmpty()) {
            HumanName humanName = NameConverter.convert(relationshipTypeCell.getString());
            contactBuilder.addContactName(humanName, relationshipWithNameCell);
        }

        AddressBuilder addressBuilder = new AddressBuilder(contactBuilder);
        CsvCell nameOfBuildingCell = parser.getRelationshipWithHouseName();
        if (!nameOfBuildingCell.isEmpty()) {
            addressBuilder.addLine(nameOfBuildingCell.getString(), nameOfBuildingCell);
        }
        CsvCell numberOfBuildingCell = parser.getRelationshipWithHouseNumber();
        CsvCell nameOfRoadCell = parser.getRelationshipWithRoad();
        addressBuilder.addLineFromHouseNumberAndRoad(numberOfBuildingCell, nameOfRoadCell);

        CsvCell nameOfLocalityCell = parser.getRelationshipWithLocality();
        if (!nameOfLocalityCell.isEmpty()) {
            addressBuilder.addLine(nameOfLocalityCell.getString(), nameOfLocalityCell);
        }
        CsvCell nameOfTownCell = parser.getRelationshipWithPostTown();
        if (!nameOfTownCell.isEmpty()) {
            addressBuilder.setTown(nameOfTownCell.getString(), nameOfTownCell);
        }
        CsvCell nameOfCountyCell = parser.getRelationshipWithCounty();
        if (!nameOfCountyCell.isEmpty()) {
            addressBuilder.setDistrict(nameOfCountyCell.getString(), nameOfCountyCell);
        }
        CsvCell fullPostCodeCell = parser.getRelationshipWithPostCode();
        if (!fullPostCodeCell.isEmpty()) {
            addressBuilder.setPostcode(fullPostCodeCell.getString(), fullPostCodeCell);
        }

        CsvCell relWithTelephone = parser.getRelationshipWithTelephone();
        if (!(relWithTelephone).isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME);
            contactPointBuilder.setValue(relWithTelephone.getString(), relWithTelephone);
        }
        CsvCell relWithWorkTelephone = parser.getRelationshipWithWorkTelephone();
        if (!(relWithWorkTelephone).isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
            contactPointBuilder.setValue(relWithWorkTelephone.getString(), relWithWorkTelephone);
        }
        CsvCell relWithMobileTelephone = parser.getRelationshipWithMobileTelephone();
        if (!(relWithWorkTelephone).isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.MOBILE);
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

        CsvCell startDateCell = parser.getDateEvent();
        if (!startDateCell.isEmpty()) {
            contactBuilder.setStartDate(startDateCell.getDate(), startDateCell);
        }

        CsvCell endDateCell = parser.getDateEnded();
        if (!endDateCell.isEmpty()) {
            contactBuilder.setEndDate(endDateCell.getDate(), endDateCell);
        }
    }
}
