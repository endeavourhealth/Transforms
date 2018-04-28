package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.endeavourhealth.transform.homerton.HomertonCsvToFhirTransformer;
import org.endeavourhealth.transform.homerton.cache.PatientResourceCache;
import org.endeavourhealth.transform.homerton.schema.PatientTable;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PatientTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper,
                                 String primaryOrgOdsCode) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    transform((PatientTable) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void transform(PatientTable parser,
                                             FhirResourceFiler fhirResourceFiler,
                                             HomertonCsvHelper csvHelper) throws Exception {

        CsvCell millenniumPersonIdCell = parser.getPersonId();
        CsvCell cnnCell = parser.getCNN();

        //store the MRN/PersonID mapping in BOTH directions
        csvHelper.saveInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, cnnCell.getString(), millenniumPersonIdCell.getString());

        csvHelper.saveInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, millenniumPersonIdCell.getString(), cnnCell.getString());

        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(millenniumPersonIdCell, csvHelper);

        IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_HOMERTON_CNN_PATIENT_ID);
        identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
        identifierBuilder.setValue(cnnCell.getString(), cnnCell);

        patientBuilder.setActive(true);

        CsvCell firstNameCell = parser.getFirstname();
        CsvCell lastNameCell = parser.getSurname();

        NameBuilder nameBuilder = new NameBuilder(patientBuilder);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addGiven(firstNameCell.getString(), firstNameCell);
        nameBuilder.addFamily(lastNameCell.getString(), lastNameCell);

        // Telecom
        CsvCell mobileCell = parser.getMobileTel();
        if (!mobileCell.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.MOBILE);
            contactPointBuilder.setValue(mobileCell.getString(), mobileCell);
        }

        CsvCell homeCell = parser.getHomeTel();
        if (!homeCell.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME);
            contactPointBuilder.setValue(homeCell.getString(), homeCell);
        }

        CsvCell workCell = parser.getWorkTel();
        if (!workCell.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
            contactPointBuilder.setValue(workCell.getString(), workCell);
        }

        // Gender
        CsvCell genderCell = parser.getGenderID();
        if (!genderCell.isEmpty()) {
            Enumerations.AdministrativeGender fhirGender = convertGenderToFHIR(genderCell.getInt().intValue());
            patientBuilder.setGender(fhirGender, genderCell);
        }

        // Ethnic group
        CsvCell ethnicityIdCell = parser.getEthnicGroupID();
        CsvCell ethnicityTermCell = parser.getEthnicGroupName();
        /*if (!ethnicityIdCell.isEmpty() || !ethnicityTermCell.isEmpty()) {
            //TODO - need to convert from Homerton ethnicity term or ID to FHIR ethnicity
            EthnicCategory fhirEthnicity = null;
            patientBuilder.setEthnicity(fhirEthnicity, cell??);
        }*/

        // Date of birth
        CsvCell dobCell = parser.getDOB();
        if (!dobCell.isEmpty()) {
            patientBuilder.setDateOfBirth(dobCell.getDateTime(), dobCell);
        }

        CsvCell dodCell = parser.getDOD();
        if (!dodCell.isEmpty()) {
            patientBuilder.setDateOfDeath(dodCell.getDateTime(), dobCell);
        }

        // GP
        CsvCell gpCell = parser.getGPID();
        if (!gpCell.isEmpty() && gpCell.getString().length() > 0) {
            ResourceId resourceId = getGPResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, gpCell.getString());
            if (resourceId == null) {
                resourceId = createGPResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, gpCell.getString());
            }
            Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, resourceId.getResourceId().toString());
            patientBuilder.addCareProvider(practitionerReference, gpCell);
        }

        // GP Practice
        CsvCell practiceCell = parser.getPracticeID();
        if (!practiceCell.isEmpty() && practiceCell.getString().length() > 0) {
            ResourceId resourceId = getGlobalOrgResourceId(practiceCell.getString());
            if (resourceId == null) {
                resourceId = createGlobalOrgResourceId(practiceCell.getString());
            }
            Reference organisationReference = ReferenceHelper.createReference(ResourceType.Organization, resourceId.getResourceId().toString());
            patientBuilder.addCareProvider(organisationReference, practiceCell);
        }

        // Address
        CsvCell line1Cell = parser.getAddressLine1();
        CsvCell line2Cell = parser.getAddressLine2();
        CsvCell line3Cell = parser.getAddressLine3();
        CsvCell cityCell = parser.getCity();
        CsvCell countyCell = parser.getCounty();
        CsvCell postcodeCell = parser.getPostcode();

        AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
        addressBuilder.setUse(Address.AddressUse.HOME);
        addressBuilder.addLine(line1Cell.getString(), line1Cell);
        addressBuilder.addLine(line2Cell.getString(), line2Cell);
        addressBuilder.addLine(line3Cell.getString(), line3Cell);
        addressBuilder.setTown(cityCell.getString(), cityCell);
        addressBuilder.setDistrict(countyCell.getString(), countyCell);
        addressBuilder.setPostcode(postcodeCell.getString(), postcodeCell);

        //fhirPatient set context
        //TODO fhirPatient.addExtension(ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT , "clinical coding"));

        if (LOG.isTraceEnabled()) {
            LOG.trace("Save PatientTable:" + FhirSerializationHelper.serializeResource(patientBuilder.getResource()));
        }
        //savePatientResourceMapIds(fhirResourceFiler, parser.getCurrentState(), patientBuilder);
    }

    /*
     *
     */
    /*public static void patientCreateOrUpdate(PatientTable parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode) throws Exception {

        org.hl7.fhir.instance.model.PatientTable fhirPatient = new org.hl7.fhir.instance.model.PatientTable();

        fhirPatient.setId(parser.getCNN());

        fhirPatient.addIdentifier(new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_HOMERTON_CNN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(parser.getCNN())));

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

        LOG.trace("Save PatientTable:" + FhirSerializationHelper.serializeResource(fhirPatient));
        savePatientResourceMapIds(fhirResourceFiler, parser.getCurrentState(), fhirPatient.getId(), fhirPatient);
    }*/

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
