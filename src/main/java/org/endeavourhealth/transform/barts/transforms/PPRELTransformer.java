package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ContactPointHelper;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.common.fhir.schema.ContactRelationship;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsCernerCodeValueRefDal;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPREL;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPRELTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPRELTransformer.class);
    private static CernerCodeValueRefDalI cernerCodeValueRefDalI = null;

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

            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.RELATIONSHIP_TO_PATIENT,
                    Long.parseLong(parser.getRelationshipToPatientCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                ContactRelationship fhirContactRelationship = ContactRelationship.fromCode(cernerCodeValueRef.getCodeDescTxt());
                fhirContactComponent.addRelationship(CodeableConceptHelper.createCodeableConcept(fhirContactRelationship));
            } else {
                LOG.warn("Relationship To Patient code: " + parser.getRelationshipToPatientCode() + " not found in Code Value lookup");
            }
        }

        if (parser.getPersonRelationTypeCode() != null && parser.getPersonRelationTypeCode().length() > 0) {

            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.PERSON_RELATIONSHIP_TYPE,
                    Long.parseLong(parser.getPersonRelationTypeCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                ContactRelationship fhirContactRelationship = ContactRelationship.fromCode(cernerCodeValueRef.getCodeDescTxt());
                fhirContactComponent.addRelationship(CodeableConceptHelper.createCodeableConcept(fhirContactRelationship));
            } else {
                LOG.warn("Person Relation Type code: " + parser.getPersonRelationTypeCode() + " not found in Code Value lookup");
            }
        }

        fhirPatient.addContact(fhirContactComponent);


        PatientResourceCache.savePatientResource(Long.parseLong(parser.getMillenniumPersonIdentifier()), fhirPatient);

    }

}
