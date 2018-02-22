package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsCernerCodeValueRefDal;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPREL;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.HumanName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPRELTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPRELTransformer.class);


    public static void transform(String version,
                                 PPREL parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry(parser);
                if (valStr == null) {
                    createPatientRelationship(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static String validateEntry(PPREL parser) {
        return null;
    }

    public static void createPatientRelationship(PPREL parser,
                                                 FhirResourceFiler fhirResourceFiler,
                                                 BartsCsvHelper csvHelper,
                                                 String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        CsvCell milleniumPersonIdCell = parser.getMillenniumPersonIdentifier();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(milleniumPersonIdCell, csvHelper);

        // If we can't find a patient resource from a previous PPATI file, throw an exception but if the line is inactive then just ignore it
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {

            //TODO - need to handle DELETING an existing relationship of a patient resource

            return;
        }

        //TODO - need to handle DELTAS to existing contacts

        PatientContactBuilder contactBuilder = new PatientContactBuilder(patientBuilder);

        CsvCell title = parser.getTitle();
        CsvCell firstName = parser.getFirstName();
        CsvCell middleName = parser.getMiddleName();
        CsvCell lastName = parser.getLastName();

        NameBuilder nameBuilder = new NameBuilder(contactBuilder);
        nameBuilder.beginName(HumanName.NameUse.USUAL);
        nameBuilder.addPrefix(title.getString(), title);
        nameBuilder.addGiven(firstName.getString(), firstName);
        nameBuilder.addGiven(middleName.getString(), middleName);
        nameBuilder.addFamily(lastName.getString(), lastName);

        CsvCell line1 = parser.getAddressLine1();
        CsvCell line2 = parser.getAddressLine2();
        CsvCell line3 = parser.getAddressLine3();
        CsvCell line4 = parser.getAddressLine4();
        CsvCell city = parser.getCity();
        CsvCell postcode = parser.getPostcode();

        AddressBuilder addressBuilder = new AddressBuilder(contactBuilder);
        addressBuilder.beginAddress(Address.AddressUse.HOME);
        addressBuilder.addLine(line1.getString(), line1);
        addressBuilder.addLine(line2.getString(), line2);
        addressBuilder.addLine(line3.getString(), line3);
        addressBuilder.addLine(line4.getString(), line4);
        addressBuilder.setTown(city.getString(), city);
        addressBuilder.setPostcode(postcode.getString(), postcode);

        CsvCell homePhone = parser.getHomePhoneNumber();
        if (!homePhone.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
            contactPointBuilder.addContactPoint();
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setValue(homePhone.getString(), homePhone);
        }

        CsvCell mobilePhone = parser.getMobilePhoneNumber();
        if (!mobilePhone.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
            contactPointBuilder.addContactPoint();
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.MOBILE);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setValue(mobilePhone.getString(), mobilePhone);
        }

        CsvCell workPhone = parser.getWorkPhoneNumber();
        if (!workPhone.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
            contactPointBuilder.addContactPoint();
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setValue(workPhone.getString(), workPhone);
        }

        CsvCell emailAddress = parser.getEmailAddress();
        if (!emailAddress.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(contactBuilder);
            contactPointBuilder.addContactPoint();
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL);
            contactPointBuilder.setValue(emailAddress.getString(), emailAddress);
        }

        CsvCell startDate = parser.getBeginEffectiveDateTime();
        if (!startDate.isEmpty()) {
            contactBuilder.setStartDate(startDate.getDate(), startDate);
        }

        CsvCell endDate = parser.getEndEffectiveDateTime();
        if (!endDate.isEmpty()) {
            contactBuilder.setEndDate(endDate.getDate(), endDate);
        }

        CsvCell relationshipToPatientCell = parser.getRelationshipToPatientCode();
        if (!relationshipToPatientCell.isEmpty()) {

            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                                                                    RdbmsCernerCodeValueRefDal.RELATIONSHIP_TO_PATIENT,
                                                                    relationshipToPatientCell.getLong(),
                                                                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                String relationshipToPatientDesc = cernerCodeValueRef.getCodeDescTxt();

                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(contactBuilder, null);
                codeableConceptBuilder.setText(relationshipToPatientDesc);
            } else {
                // LOG.warn("Relationship To Patient code: " + parser.getRelationshipToPatientCode() + " not found in Code Value lookup");
            }
        }

        CsvCell relationshipTypeCell = parser.getPersonRelationTypeCode();
        if (!relationshipTypeCell.isEmpty()) {

            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.PERSON_RELATIONSHIP_TYPE,
                    relationshipTypeCell.getLong(),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                String relationshipTypeDesc = cernerCodeValueRef.getCodeDescTxt();

                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(contactBuilder, null);
                codeableConceptBuilder.setText(relationshipTypeDesc);
            } else {
                // LOG.warn("Relationship To Patient code: " + parser.getRelationshipToPatientCode() + " not found in Code Value lookup");
            }
        }
    }

    /*public static void createPatientRelationship(PPREL parser,
                                                 FhirResourceFiler fhirResourceFiler,
                                                 BartsCsvHelper csvHelper,
                                                 String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {



        if (cernerCodeValueRefDalI == null) {
            cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
        }

        Patient fhirPatient = PatientResourceCache.getPatientResource(Long.parseLong(parser.getMillenniumPersonIdentifier()));

        // If we can't find a patient resource from a previous PPATI file, throw an exception but if the line is inactive then just ignore it
        if (fhirPatient == null) {
            if (parser.isActive()) {
                LOG.warn("Patient Resource Not Found In Cache: " + parser.getMillenniumPersonIdentifier());
            } else {
                return;
            }
        }

        // Patient Address
        Patient.ContactComponent fhirContactComponent = new Patient.ContactComponent();

        if (parser.getFirstName() != null && parser.getFirstName().length() > 0) {
            HumanName name = org.endeavourhealth.common.fhir.NameConverter.createHumanName(
                    HumanName.NameUse.USUAL,
                    parser.getTitle(), parser.getFirstName(), parser.getMiddleName(),
                    parser.getLastName());

            fhirContactComponent.setName(name);
        }

        Address fhirRelationAddress = AddressConverter.createAddress(Address.AddressUse.HOME, parser.getAddressLine1(),
                parser.getAddressLine2(), parser.getAddressLine3(), parser.getAddressLine4(), parser.getCountry(), parser.getPostcode());

        fhirContactComponent.setAddress(fhirRelationAddress);


        if (parser.getHomePhoneNumber() != null && parser.getHomePhoneNumber().length() > 0 ) {
            ContactPoint contactPoint = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE,
                    ContactPoint.ContactPointUse.HOME, parser.getHomePhoneNumber());

            fhirContactComponent.addTelecom(contactPoint);
        }

        if (parser.getMobilePhoneNumber() != null && parser.getMobilePhoneNumber().length() > 0 ) {
            ContactPoint contactPoint = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE,
                    ContactPoint.ContactPointUse.MOBILE, parser.getMobilePhoneNumber());

            fhirContactComponent.addTelecom(contactPoint);
        }

        if (parser.getWorkPhoneNumber() != null && parser.getWorkPhoneNumber().length() > 0 ) {
            ContactPoint contactPoint = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE,
                    ContactPoint.ContactPointUse.WORK, parser.getWorkPhoneNumber());

            fhirContactComponent.addTelecom(contactPoint);
        }

        if (parser.getEmailAddress() != null && parser.getEmailAddress().length() > 0 ) {
            ContactPoint contactPoint = ContactPointHelper.create(ContactPoint.ContactPointSystem.EMAIL,
                    ContactPoint.ContactPointUse.HOME, parser.getEmailAddress());

            fhirContactComponent.addTelecom(contactPoint);
        }

        if (parser.getBeginEffectiveDateTime() != null || parser.getEndEffectiveDateTime() != null) {
            Period fhirPeriod = PeriodHelper.createPeriod(parser.getBeginEffectiveDateTime(), parser.getEndEffectiveDateTime());
            fhirContactComponent.setPeriod(fhirPeriod);
        }

        if (parser.getRelationshipToPatientCode() != null && parser.getRelationshipToPatientCode().length() >0) {

            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.RELATIONSHIP_TO_PATIENT,
                    Long.parseLong(parser.getRelationshipToPatientCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                ContactRelationship fhirContactRelationship = ContactRelationship.fromCode(cernerCodeValueRef.getCodeDescTxt());
                fhirContactComponent.addRelationship(CodeableConceptHelper.createCodeableConcept(fhirContactRelationship));
            } else {
                // LOG.warn("Relationship To Patient code: " + parser.getRelationshipToPatientCode() + " not found in Code Value lookup");
            }
        }

        if (parser.getPersonRelationTypeCode() != null && parser.getPersonRelationTypeCode().length() > 0) {

            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.PERSON_RELATIONSHIP_TYPE,
                    Long.parseLong(parser.getPersonRelationTypeCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                ContactRelationship fhirContactRelationship = ContactRelationship.fromCode(cernerCodeValueRef.getCodeDescTxt());
                fhirContactComponent.addRelationship(CodeableConceptHelper.createCodeableConcept(fhirContactRelationship));
            } else {
                // LOG.warn("Person Relation Type code: " + parser.getPersonRelationTypeCode() + " not found in Code Value lookup");
            }
        }

        fhirPatient.addContact(fhirContactComponent);


        PatientResourceCache.savePatientResource(Long.parseLong(parser.getMillenniumPersonIdentifier()), fhirPatient);

    }*/

}
