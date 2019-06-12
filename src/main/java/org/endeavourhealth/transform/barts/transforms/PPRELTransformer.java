package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PPREL;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PPRELTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPRELTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                createPatientRelationship((PPREL)parser, fhirResourceFiler, csvHelper);
            }
        }
    }


    public static void createPatientRelationship(PPREL parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //related person is the ID we should be using
        CsvCell relationshipIdCell = parser.getRelatedPersonMillenniumIdentifier();
        //CsvCell relationshipIdCell = parser.getMillenniumPersonRelationId();

        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        //if non-active (i.e. deleted) we should REMOVE the contactPoint. Unlike all the other PPxxx files, we DO
        //get the person ID when a PPREL is non-active, so we don't need to mess about with the internal_id_map to find it
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {

            if (!personIdCell.isEmpty()) {
                PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell);
                if (patientBuilder != null) {
                    PatientContactBuilder.removeExistingContactPointById(patientBuilder, relationshipIdCell.getString());

                    csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
                }
            }
            return;
        }

        //if ended, we also remove from the FHIR patient
        CsvCell endDate = parser.getEndEffectiveDateTime();
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDate)) {
            PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell);
            if (patientBuilder != null) {
                PatientContactBuilder.removeExistingContactPointById(patientBuilder, relationshipIdCell.getString());

                csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
            }
            return;
        }

        //store the relationship type in the internal ID map table so the family history transformer can look it up
        //moved this to PPREL PRE transformer, but only after the PP... bulk files have been processed
        /*CsvCell relationshipToPatientCell = parser.getRelationshipToPatientCode();
        csvHelper.savePatientRelationshipType(personIdCell, relationshipIdCell, relationshipToPatientCell);*/

        CsvCell title = parser.getTitle();
        CsvCell firstName = parser.getFirstName();
        CsvCell middleName = parser.getMiddleName();
        CsvCell lastName = parser.getLastName();
        CsvCell line1 = parser.getAddressLine1();
        CsvCell line2 = parser.getAddressLine2();
        CsvCell line3 = parser.getAddressLine3();
        CsvCell line4 = parser.getAddressLine4();
        CsvCell city = parser.getCity();
        CsvCell postcode = parser.getPostcode();
        CsvCell homePhone = parser.getHomePhoneNumber();
        CsvCell mobilePhone = parser.getMobilePhoneNumber();
        CsvCell workPhone = parser.getWorkPhoneNumber();
        CsvCell emailAddress = parser.getEmailAddress();

        //the PPREL file contains recorss for phone family members (with names, addresses etc.) but also empty
        //records that are just used to record family history. Don't bother adding these empty relationships to the patient record
        if (title.isEmpty()
                && firstName.isEmpty()
                && middleName.isEmpty()
                && lastName.isEmpty()
                && line1.isEmpty()
                && line2.isEmpty()
                && line3.isEmpty()
                && line4.isEmpty()
                && city.isEmpty()
                && postcode.isEmpty()
                && homePhone.isEmpty()
                && mobilePhone.isEmpty()
                && workPhone.isEmpty()
                && emailAddress.isEmpty()) {
            return;
        }

        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell);
        if (patientBuilder == null) {
            return;
        }

        try {

            //attempt to re-use the patient contact, otherwise we're contantly changing the order of them in the patient
            PatientContactBuilder contactBuilder = PatientContactBuilder.findOrCreateForId(patientBuilder, relationshipIdCell);
            contactBuilder.reset(); //we get the full record, so clear down before populating

            /*//we always fully recreate the patient contact from the Barts record, so just remove any existing contact that matches on ID
            PatientContactBuilder.removeExistingContactPointById(patientBuilder, relationshipIdCell.getString());

            PatientContactBuilder contactBuilder = new PatientContactBuilder(patientBuilder);
            contactBuilder.setId(relationshipIdCell.getString(), relationshipIdCell);*/

            NameBuilder nameBuilder = new NameBuilder(contactBuilder);
            nameBuilder.setUse(HumanName.NameUse.USUAL);
            nameBuilder.addPrefix(title.getString(), title);
            nameBuilder.addGiven(firstName.getString(), firstName);
            nameBuilder.addGiven(middleName.getString(), middleName);
            nameBuilder.addFamily(lastName.getString(), lastName);

            if (!line1.isEmpty()
                    || !line2.isEmpty()
                    || !line3.isEmpty()
                    || !line4.isEmpty()
                    || !city.isEmpty()
                    || !postcode.isEmpty()) {

                AddressBuilder addressBuilder = new AddressBuilder(contactBuilder);
                addressBuilder.setUse(Address.AddressUse.HOME);
                addressBuilder.addLine(line1.getString(), line1);
                addressBuilder.addLine(line2.getString(), line2);
                addressBuilder.addLine(line3.getString(), line3);
                addressBuilder.addLine(line4.getString(), line4);
                addressBuilder.setCity(city.getString(), city);
                addressBuilder.setPostcode(postcode.getString(), postcode);
            }

            if (!homePhone.isEmpty()) {
                ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
                contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME, homePhone);
                contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE, homePhone);
                contactPointBuilder.setValue(homePhone.getString(), homePhone);
            }

            if (!mobilePhone.isEmpty()) {
                ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
                contactPointBuilder.setUse(ContactPoint.ContactPointUse.MOBILE, mobilePhone);
                contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE, mobilePhone);
                contactPointBuilder.setValue(mobilePhone.getString(), mobilePhone);
            }

            if (!workPhone.isEmpty()) {
                ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
                contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK, workPhone);
                contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE, workPhone);
                contactPointBuilder.setValue(workPhone.getString(), workPhone);
            }

            if (!emailAddress.isEmpty()) {
                ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
                contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME, emailAddress);
                contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL, emailAddress);
                contactPointBuilder.setValue(emailAddress.getString(), emailAddress);
            }

            CsvCell startDate = parser.getBeginEffectiveDateTime();
            if (!BartsCsvHelper.isEmptyOrIsStartOfTime(startDate)) {
                Date d = BartsCsvHelper.parseDate(startDate);
                contactBuilder.setStartDate(d, startDate);
            }

            //ended relationships are now removed from the FHIR patient resource. Huge numbers of PPREL records exist
            //for each person meaning the FHIR patient resources get filled up with a vast amount of past data,
            //so we treat ended ones the same as deleted, and remove them from the patient resource
            /*CsvCell endDate = parser.getEndEffectiveDateTime();
            if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDate)) {
                Date d = BartsCsvHelper.parseDate(endDate);
                contactBuilder.setEndDate(d, endDate);
            }*/

            //the PPREL file has two codes, one defining the relationship to the patient (e.g. self, mother)
            //and one defining the type of relationship (e.g. next of kin). In typical Cerner style, this isn't
            //as clear-cut as it sounds and they often have the same thing, so we combine both into the same
            //field and remove duplicates
            List<String> types = new ArrayList<>();
            List<CsvCell> cells = new ArrayList<>();

            CsvCell relationshipToPatientCell = parser.getRelationshipToPatientCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(relationshipToPatientCell)) {

                CsvCell cvRefCell = BartsCodeableConceptHelper.getCellDesc(csvHelper, CodeValueSet.RELATIONSHIP_TO_PATIENT, relationshipToPatientCell);
                String cvRefStr = cvRefCell.getString();

                types.add(cvRefStr);
                cells.add(relationshipToPatientCell);
                cells.add(cvRefCell);
            }

            CsvCell relationshipTypeCell = parser.getPersonRelationTypeCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(relationshipTypeCell)) {

                CsvCell cvRefCell = BartsCodeableConceptHelper.getCellDesc(csvHelper, CodeValueSet.PERSON_RELATIONSHIP_TYPE, relationshipTypeCell);
                String cvRefStr = cvRefCell.getString();

                //often both fields point to the same term, so don't duplicate it
                if (!types.contains(cvRefStr)) {
                    types.add(cvRefStr);
                }
                cells.add(relationshipTypeCell);
                cells.add(cvRefCell);
            }

            if (!types.isEmpty()) {
                String typeStr = String.join(", ", types);

                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(contactBuilder, CodeableConceptBuilder.Tag.Patient_Contact_Relationship);
                codeableConceptBuilder.setText(typeStr, cells.toArray(new CsvCell[]{}));
            }


            //PPNAM transformer removes any names added by the ADT transform, so we would need to do the same if the ADT
            //feed populated relationships. In place of that, just validate that there are no relationships without IDs
            //in case the ADT feed is ever updated to add them - this will highlight that we need to remove duplciates here
            checkForRelationshipsWithoutIds(patientBuilder);

        } finally {
            //no need to save the resource now, as all patient resources are saved at the end of the PP... files
            csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
        }
    }

    private static void checkForRelationshipsWithoutIds(PatientBuilder patientBuilder) throws Exception {
        Patient patient = (Patient)patientBuilder.getResource();
        if (!patient.hasContact()) {
            return;
        }

        for (Patient.ContactComponent contact: patient.getContact()) {
            if (!contact.hasId()) {
                throw new Exception("Patient " + patient.getId() + " has a relationship without an ID (does ADT feed now populate relationships?)");
            }
        }
    }


}
