package org.endeavourhealth.transform.homerton.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.homerton.schema.Patient;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatientTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    public static void transform(String version,
                                 Patient parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode) throws Exception {

        // Skip header line
        parser.nextRecord();

        while (parser.nextRecord()) {
            try {
                patientCreateOrUpdate(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }


    /*
     *
     */
    public static void patientCreateOrUpdate(Patient parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode) throws Exception {

        org.hl7.fhir.instance.model.Patient fhirPatient = new org.hl7.fhir.instance.model.Patient();

        fhirPatient.setId(parser.getCNN());

        fhirPatient.addIdentifier(new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_HOMERTON_CNN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(parser.getCNN())));

        fhirPatient.setActive(true);

        HumanName name = org.endeavourhealth.common.fhir.NameConverter.createHumanName(HumanName.NameUse.OFFICIAL, null, parser.getFirstname(), "", parser.getSurname());
        fhirPatient.addName(name);

        // Telecom
        if (parser.getMobileTel() != null && parser.getMobileTel().length() > 0) {
            ContactPoint contactPoint = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE, ContactPoint.ContactPointUse.MOBILE, parser.getMobileTel());
            fhirPatient.addTelecom(contactPoint);
        }

        if (parser.getHomeTel() != null && parser.getHomeTel().length() > 0) {
            ContactPoint contactPoint = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE, ContactPoint.ContactPointUse.HOME, parser.getHomeTel());
            fhirPatient.addTelecom(contactPoint);
        }

        if (parser.getWorkTel() != null && parser.getWorkTel().length() > 0) {
            ContactPoint contactPoint = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE, ContactPoint.ContactPointUse.WORK, parser.getWorkTel());
            fhirPatient.addTelecom(contactPoint);
        }

        // Gender
        fhirPatient.setGender(convertGenderToFHIR(parser.getGenderID()));

        // Ethnic group
        CodeableConcept ethnicGroup = ethnicGroup = new CodeableConcept();
        ethnicGroup.addCoding().setCode(parser.getEthnicGroupID()).setSystem(FhirExtensionUri.PATIENT_ETHNICITY).setDisplay(parser.getEthnicGroupName());
        Extension ex = new Extension();
        ex.setUrl(FhirExtensionUri.PATIENT_ETHNICITY);
        ex.setValue(ethnicGroup);
        fhirPatient.addExtension(ex);

        // Date of birth
        if (parser.getDOB() != null) {
            fhirPatient.setBirthDate(parser.getDOB());
        }

        if (parser.getDOD() != null) {
            fhirPatient.setDeceased(new DateTimeType(parser.getDOD()));
        }

        // GP
        if (parser.getGPID() != null && parser.getGPID().length() > 0) {
            fhirPatient.addCareProvider(ReferenceHelper.createReference(ResourceType.Practitioner, parser.getGPID()));
        }

        // GP Practice
        if (parser.getPracticeID() != null && parser.getPracticeID().length() > 0) {
            fhirPatient.addCareProvider(ReferenceHelper.createReference(ResourceType.Organization, parser.getPracticeID()));
        }

        // Address
        Address fhirAddress = AddressConverter.createAddress(Address.AddressUse.HOME, parser.getAddressLine1(), parser.getAddressLine2(), parser.getAddressLine3(), parser.getCity(), parser.getCounty(), parser.getPostcode());
        fhirPatient.addAddress(fhirAddress);

        //fhirPatient set context
        //TODO fhirPatient.addExtension(ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT , "clinical coding"));

        LOG.trace("Save Patient:" + FhirSerializationHelper.serializeResource(fhirPatient));
        savePatientResourceMapIds(fhirResourceFiler, parser.getCurrentState(), fhirPatient.getId(), fhirPatient);
    }

    /*
     *
     */
    public static Enumerations.AdministrativeGender convertGenderToFHIR(int gender) {
        if (gender == 1) {
            return Enumerations.AdministrativeGender.FEMALE;
        } else {
            if (gender == 2) {
                return Enumerations.AdministrativeGender.MALE;
            } else {
                if (gender == 9) {
                    return Enumerations.AdministrativeGender.NULL;
                } else {
                    return Enumerations.AdministrativeGender.UNKNOWN;
                }
            }
        }
    }


}
