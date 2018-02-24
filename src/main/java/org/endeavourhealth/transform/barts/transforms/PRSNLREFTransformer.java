package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
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
        ResourceId practitionerResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, personnelIdCell);

        practitionerBuilder.setId(practitionerResourceId.getResourceId().toString(), personnelIdCell);

        CsvCell activeCell = parser.getActiveIndicator();
        practitionerBuilder.setActive(activeCell.getIntAsBoolean(), activeCell);

        PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder);

        CsvCell positionCode = parser.getMilleniumPositionCode();
        BartsCodeableConceptHelper.applyCodeDisplayTxt(positionCode, CernerCodeValueRef.PERSONNEL_POSITION, roleBuilder, PractitionerRoleBuilder.TAG_ROLE_CODEABLE_CONCEPT, fhirResourceFiler);

        CsvCell specialityCode = parser.getMillenniumSpecialtyCode();
        BartsCodeableConceptHelper.applyCodeDisplayTxt(specialityCode, CernerCodeValueRef.PERSONNEL_SPECIALITY, roleBuilder, PractitionerRoleBuilder.TAG_SPECIALTY_CODEABLE_CONCEPT, fhirResourceFiler);

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
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);
            identifierBuilder.setValue(gmpCode.getString(), gmpCode);
        }

        CsvCell consultantNHSCode = parser.getConsultantNHSCode();
        if (!consultantNHSCode.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CONSULTANT_CODE);
            identifierBuilder.setValue(consultantNHSCode.getString(), consultantNHSCode);
        }

        LOG.debug("Save Practitioner (PersonnelId=" + personnelIdCell.getString() + "):" + FhirSerializationHelper.serializeResource(practitionerBuilder.getResource()));
        saveAdminResource(fhirResourceFiler, parser.getCurrentState(), practitionerBuilder);
    }

}
