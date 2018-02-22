package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsCernerCodeValueRefDal;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.PRSNLREF;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.endeavourhealth.common.fhir.NameConverter.createHumanName;

public class PRSNLREFTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PRSNLREFTransformer.class);
    private static CernerCodeValueRefDalI cernerCodeValueRefDalI = null;

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
                CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(RdbmsCernerCodeValueRefDal.PERSONNEL_POSITION, positionCode, fhirResourceFiler.getServiceId());
                if (cernerCodeValueRef != null) {
                    String positionName = cernerCodeValueRef.getCodeDispTxt();
                    CodeableConcept positionTypeCode = CodeableConceptHelper.createCodeableConcept(BartsCsvToFhirTransformer.CODE_SYSTEM_PERSONNEL_POSITION_TYPE, positionName, positionCode.toString());
                    fhirRole.setRole(positionTypeCode);
                } else {
                    // LOG.warn("Position code: "+positionCode+" not found in Code Value lookup");
                }
            }
            if (specialityCode != null) {
                CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(RdbmsCernerCodeValueRefDal.PERSONNEL_SPECIALITY, specialityCode, fhirResourceFiler.getServiceId());
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
    }
}
