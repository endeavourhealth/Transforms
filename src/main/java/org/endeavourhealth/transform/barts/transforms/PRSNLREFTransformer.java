package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.PRSNLREF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.HumanName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PRSNLREFTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PRSNLREFTransformer.class);

    public static void transform(String version,
                                 PRSNLREF parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        while (parser.nextRecord()) {
            try {
                createPractitioner(parser, fhirResourceFiler, csvHelper);
            }
            catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createPractitioner(PRSNLREF parser,
                                           FhirResourceFiler fhirResourceFiler,
                                           BartsCsvHelper csvHelper) throws Exception {

        PractitionerBuilder practitionerBuilder = new PractitionerBuilder();

        CsvCell personnelIdCell = parser.getPersonnelID();

        // this Practitioner resource id
        ResourceId practitionerResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, personnelIdCell.getString());
        if (practitionerResourceId == null) {
            practitionerResourceId = createPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, personnelIdCell.getString());
        }

        practitionerBuilder.setId(practitionerResourceId.getResourceId().toString(), personnelIdCell);

        CsvCell activeCell = parser.getActiveIndicator();
        practitionerBuilder.setActive(activeCell.getIntAsBoolean(), activeCell);

        PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder);

        CsvCell positionCode = parser.getMilleniumPositionCode();
        if (!positionCode.isEmpty()) {

            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(CernerCodeValueRef.PERSONNEL_POSITION, positionCode.getLong(), fhirResourceFiler.getServiceId());
            if (cernerCodeValueRef != null) {
                String positionName = cernerCodeValueRef.getCodeDispTxt();

                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(roleBuilder, PractitionerRoleBuilder.TAG_ROLE_CODEABLE_CONCEPT);
                codeableConceptBuilder.addCoding(BartsCsvToFhirTransformer.CODE_SYSTEM_PERSONNEL_POSITION_TYPE);
                codeableConceptBuilder.setCodingCode(positionCode.getString(), positionCode);
                codeableConceptBuilder.setCodingDisplay(positionName);

            } else {
                // LOG.warn("Position code: "+positionCode+" not found in Code Value lookup");
            }
        }

        CsvCell specialityCode = parser.getMillenniumSpecialtyCode();
        if (!specialityCode.isEmpty()) {
            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(CernerCodeValueRef.PERSONNEL_SPECIALITY, specialityCode.getLong(), fhirResourceFiler.getServiceId());
            if (cernerCodeValueRef != null) {
                String specialityName = cernerCodeValueRef.getCodeDispTxt();

                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(roleBuilder, PractitionerRoleBuilder.TAG_SPECIALTY_CODEABLE_CONCEPT);
                codeableConceptBuilder.addCoding(BartsCsvToFhirTransformer.CODE_SYSTEM_PERSONNEL_SPECIALITY_TYPE);
                codeableConceptBuilder.setCodingCode(specialityCode.getString(), specialityCode);
                codeableConceptBuilder.setCodingDisplay(specialityName);

            } else {
                // LOG.warn("Speciality code: "+specialityCode+" not found in Code Value lookup");
            }
        }

        CsvCell title = parser.getTitle();
        CsvCell givenName = parser.getFirstName();
        CsvCell middleName = parser.getMiddleName();
        CsvCell surname = parser.getLastName();

        NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addPrefix(title.getString(), title);
        nameBuilder.addGiven(givenName.getString(), givenName);
        nameBuilder.addGiven(middleName.getString(), middleName);
        nameBuilder.addFamily(surname.getString(), surname);

        CsvCell line1 = parser.getAddress1();
        CsvCell line2 = parser.getAddress2();
        CsvCell line3 = parser.getAddress3();
        CsvCell line4 = parser.getAddress4();
        CsvCell city = parser.getCity();
        CsvCell postcode = parser.getPostCode();

        AddressBuilder addressBuilder = new AddressBuilder(practitionerBuilder);
        addressBuilder.addLine(line1.getString(), line1);
        addressBuilder.addLine(line2.getString(), line2);
        addressBuilder.addLine(line3.getString(), line3);
        addressBuilder.addLine(line4.getString(), line4);
        addressBuilder.setTown(city.getString(), city);
        addressBuilder.setPostcode(postcode.getString(), postcode);

        CsvCell email = parser.getEmail();
        if (!email.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(practitionerBuilder);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL);
            contactPointBuilder.setValue(email.getString(), email);
        }

        CsvCell phone = parser.getPhone();
        if (!phone.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(practitionerBuilder);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setValue(phone.getString(), phone);
        }

        CsvCell fax = parser.getFax();
        if (!fax.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(practitionerBuilder);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.FAX);
            contactPointBuilder.setValue(fax.getString(), fax);
        }

        CsvCell gmpCode = parser.getGPNHSCode();
        if (!gmpCode.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);
            identifierBuilder.setValue(gmpCode.getString(), gmpCode);
        }

        CsvCell consultantNHSCode = parser.getConsultantNHSCode();
        if (!consultantNHSCode.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirUri.IDENTIFIER_SYSTEM_CONSULTANT_CODE);
            identifierBuilder.setValue(consultantNHSCode.getString(), consultantNHSCode);
        }

        LOG.debug("Save Practitioner (PersonnelId=" + personnelIdCell.getString() + "):" + FhirSerializationHelper.serializeResource(practitionerBuilder.getResource()));
        saveAdminResource(fhirResourceFiler, parser.getCurrentState(), practitionerBuilder);
    }

    /*private static void createPractitioner(PRSNLREF parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper) throws Exception {

        if (cernerCodeValueRefDalI == null) {
            cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
        }

        Practitioner fhirPractitioner = new Practitioner();
        fhirPractitioner.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PRACTITIONER));

        String personnelID = parser.getPersonnelID();
        // this Practitioner resource id
        ResourceId practitionerResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, personnelID);
        if (practitionerResourceId == null) {
            practitionerResourceId = createPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, personnelID);
        }
        fhirPractitioner.setId(practitionerResourceId.getResourceId().toString());
        fhirPractitioner.setActive(parser.isActive());

        Long positionCode = parser.getMilleniumPositionCode();
        Long specialityCode = parser.getMillenniumSpecialtyCode();
        if (positionCode != null || specialityCode != null) {
            Practitioner.PractitionerPractitionerRoleComponent fhirRole = fhirPractitioner.addPractitionerRole();

            if (positionCode != null) {
                CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(RdbmsCernerCodeValueRefDal.PERSONNEL_POSITION, positionCode, fhirResourceFiler.getServiceId());
                if (cernerCodeValueRef != null) {
                    String positionName = cernerCodeValueRef.getCodeDispTxt();
                    CodeableConcept positionTypeCode = CodeableConceptHelper.createCodeableConcept(BartsCsvToFhirTransformer.CODE_SYSTEM_PERSONNEL_POSITION_TYPE, positionName, positionCode.toString());
                    fhirRole.setRole(positionTypeCode);
                } else {
                    // LOG.warn("Position code: "+positionCode+" not found in Code Value lookup");
                }
            }
            if (specialityCode != null) {
                CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(RdbmsCernerCodeValueRefDal.PERSONNEL_SPECIALITY, specialityCode, fhirResourceFiler.getServiceId());
                if (cernerCodeValueRef != null) {
                    String specialityName = cernerCodeValueRef.getCodeDispTxt();
                    CodeableConcept specialityTypeCode = CodeableConceptHelper.createCodeableConcept(BartsCsvToFhirTransformer.CODE_SYSTEM_PERSONNEL_SPECIALITY_TYPE, specialityName, specialityCode.toString());
                    fhirRole.addSpecialty(specialityTypeCode);
                } else {
                    // LOG.warn("Speciality code: "+specialityCode+" not found in Code Value lookup");
                }
            }
        }

        String title = parser.getTitle();
        String givenName = parser.getFirstName();
        String middleName = parser.getMiddleName();
        String surname = parser.getLastName();

        if (Strings.isNullOrEmpty(surname)) {
            surname = givenName;
            givenName = "";
        }
        if (Strings.isNullOrEmpty(surname)) {
            surname = "Unknown";
        }
        fhirPractitioner.setName(createHumanName(HumanName.NameUse.OFFICIAL, title, givenName, middleName, surname));

        Address address = fhirPractitioner.addAddress();
        address.addLine(parser.getAddress1());
        address.addLine(parser.getAddress2());
        address.addLine(parser.getAddress3());
        address.setDistrict(parser.getAddress4());
        address.setCity(parser.getCity());
        address.setPostalCode(parser.getPostCode());

        String email = parser.getEmail();
        if (!Strings.isNullOrEmpty(email)) {
            fhirPractitioner.addTelecom()
                    .setUse(ContactPoint.ContactPointUse.WORK)
                    .setSystem(ContactPoint.ContactPointSystem.EMAIL)
                    .setValue(email);
        }
        String phone = parser.getPhone();
        if (!Strings.isNullOrEmpty(phone)) {
            fhirPractitioner.addTelecom()
                    .setUse(ContactPoint.ContactPointUse.WORK)
                    .setSystem(ContactPoint.ContactPointSystem.PHONE)
                    .setValue(phone);
        }
        String fax = parser.getFax();
        if (!Strings.isNullOrEmpty(fax)) {
            fhirPractitioner.addTelecom()
                    .setUse(ContactPoint.ContactPointUse.WORK)
                    .setSystem(ContactPoint.ContactPointSystem.FAX)
                    .setValue(fax);
        }

        String gmpCode = parser.getGPNHSCode();
        if (!Strings.isNullOrEmpty(gmpCode)) {
            Identifier identifier = new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE).setValue(gmpCode);
            fhirPractitioner.addIdentifier(identifier);
        }

        String consultantNHSCode = parser.getConsultantNHSCode();
        if (!Strings.isNullOrEmpty(consultantNHSCode)) {
            Identifier identifier = new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_CONSULTANT_CODE).setValue(consultantNHSCode);
            fhirPractitioner.addIdentifier(identifier);
        }

        LOG.debug("Save Practitioner (PersonnelId=" + personnelID + "):" + FhirSerializationHelper.serializeResource(fhirPractitioner));
        saveAdminResource(fhirResourceFiler, parser.getCurrentState(), fhirPractitioner);
    }*/
}
