package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        //if non-active (i.e. deleted) we should REMOVE the contactPoint, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the contactPoint
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {

            String personIdStr = csvHelper.getInternalId(PPRELPreTransformer.PPREL_ID_TO_PERSON_ID, relationshipIdCell.getString());
            if (!Strings.isNullOrEmpty(personIdStr)) {

                PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(Long.valueOf(personIdStr), csvHelper);
                if (patientBuilder != null) {
                    PatientContactBuilder.removeExistingContactPointById(patientBuilder, relationshipIdCell.getString());

                    csvHelper.getPatientCache().returnPatientBuilder(Long.valueOf(personIdStr), patientBuilder);
                }
            }
            return;
        }

        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

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

        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        //we always fully recreate the patient contact from the Barts record, so just remove any existing contact that matches on ID
        PatientContactBuilder.removeExistingContactPointById(patientBuilder, relationshipIdCell.getString());


        PatientContactBuilder contactBuilder = new PatientContactBuilder(patientBuilder);
        contactBuilder.setId(relationshipIdCell.getString(), relationshipIdCell);

        NameBuilder nameBuilder = new NameBuilder(contactBuilder);
        nameBuilder.setUse(HumanName.NameUse.USUAL);
        nameBuilder.addPrefix(title.getString(), title);
        nameBuilder.addGiven(firstName.getString(), firstName);
        nameBuilder.addGiven(middleName.getString(), middleName);
        nameBuilder.addFamily(lastName.getString(), lastName);

        AddressBuilder addressBuilder = new AddressBuilder(contactBuilder);
        addressBuilder.setUse(Address.AddressUse.HOME);
        addressBuilder.addLine(line1.getString(), line1);
        addressBuilder.addLine(line2.getString(), line2);
        addressBuilder.addLine(line3.getString(), line3);
        addressBuilder.addLine(line4.getString(), line4);
        addressBuilder.setTown(city.getString(), city);
        addressBuilder.setPostcode(postcode.getString(), postcode);

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
        if (!startDate.isEmpty()) {
            Date d = BartsCsvHelper.parseDate(startDate);
            contactBuilder.setStartDate(d, startDate);
        }

        CsvCell endDate = parser.getEndEffectiveDateTime();
        //use this function to test the endDate cell, since it will have the Cerner end of time content
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDate)) {
            Date d = BartsCsvHelper.parseDate(endDate);
            contactBuilder.setEndDate(d, endDate);
        }

        CsvCell relationshipToPatientCell = parser.getRelationshipToPatientCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(relationshipToPatientCell)) {

            CsvCell relationshupToPatientDescCell = BartsCodeableConceptHelper.getCellDesc(csvHelper, CodeValueSet.RELATIONSHIP_TO_PATIENT, relationshipToPatientCell);
            String relationshipToPatientDesc = relationshupToPatientDescCell.getString();

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(contactBuilder, CodeableConceptBuilder.Tag.Patient_Contact_Relationship);
            codeableConceptBuilder.setText(relationshipToPatientDesc, relationshupToPatientDescCell);
        }

        CsvCell relationshipTypeCell = parser.getPersonRelationTypeCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(relationshipTypeCell)) {

            CsvCell relationshipTypeDescCell = BartsCodeableConceptHelper.getCellDesc(csvHelper, CodeValueSet.PERSON_RELATIONSHIP_TYPE, relationshipTypeCell);
            String relationshipTypeDesc = relationshipTypeDescCell.getString();

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(contactBuilder, CodeableConceptBuilder.Tag.Patient_Contact_Relationship);
            codeableConceptBuilder.setText(relationshipTypeDesc, relationshipTypeDescCell);
        }

        //no need to save the resource now, as all patient resources are saved at the end of the PP... files
        csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
    }


}
