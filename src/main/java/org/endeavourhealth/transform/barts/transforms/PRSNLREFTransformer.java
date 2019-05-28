package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PRSNLREF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PRSNLREFTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PRSNLREFTransformer.class);

    public static final String MAPPING_ID_PERSONNEL_NAME_TO_ID = "PersonnelNameToId";
    public static final String MAPPING_ID_CONSULTANT_TO_ID = "ConsultantToId";


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createPractitioner((PRSNLREF) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createPractitioner(PRSNLREF parser,
                                           FhirResourceFiler fhirResourceFiler,
                                           BartsCsvHelper csvHelper) throws Exception {

        PractitionerBuilder practitionerBuilder = null;

        //we share practitioner resources with the ADT feed, so make sure to retrieve an existing one to update
        CsvCell personnelIdCell = parser.getPersonnelID();
        Practitioner existingPractitioner = (Practitioner)csvHelper.retrieveResourceForLocalId(ResourceType.Practitioner, personnelIdCell);
        if (existingPractitioner == null) {
            practitionerBuilder = new PractitionerBuilder();
            practitionerBuilder.setId(personnelIdCell.getString(), personnelIdCell);

        } else {
            practitionerBuilder = new PractitionerBuilder(existingPractitioner);
        }


        //unlike other resources, we don't DELETE practitioner ones, since they may be lots of data referring to them
        CsvCell activeCell = parser.getActiveIndicator();
        practitionerBuilder.setActive(activeCell.getIntAsBoolean(), activeCell);

        //we only support one role on Practitioners for Barts, so always reuse the role object if it's present
        //this also means we don't lose anything already on the role that we won't be able to replace (e.g. parent org) because it's only present from the ADT feed
        PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder, practitionerBuilder.getRole());

        //remove specialty and role because we always replace them
        CodeableConceptBuilder.removeExistingCodeableConcept(roleBuilder, CodeableConceptBuilder.Tag.Practitioner_Role, roleBuilder.getRole());
        CodeableConceptBuilder.removeExistingCodeableConcept(roleBuilder, CodeableConceptBuilder.Tag.Practitioner_Specialty, roleBuilder.getSpecialty());

        CsvCell positionCode = parser.getMilleniumPositionCode();
        BartsCodeableConceptHelper.applyCodeDisplayTxt(positionCode, CodeValueSet.PERSONNEL_POSITION, roleBuilder, CodeableConceptBuilder.Tag.Practitioner_Role, csvHelper);

        CsvCell specialityCode = parser.getMillenniumSpecialtyCode();
        BartsCodeableConceptHelper.applyCodeDisplayTxt(specialityCode, CodeValueSet.SPECIALITY, roleBuilder, CodeableConceptBuilder.Tag.Practitioner_Specialty, csvHelper);

        CsvCell title = parser.getTitle();
        CsvCell givenName = parser.getFirstName();
        CsvCell middleName = parser.getMiddleName();
        CsvCell surname = parser.getLastName();

        NameBuilder.removeExistingNames(practitionerBuilder);

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


        AddressBuilder.removeExistingAddresses(practitionerBuilder);

        if (!line1.isEmpty()
                || !line2.isEmpty()
                || !line3.isEmpty()
                || !line4.isEmpty()
                || !city.isEmpty()
                || !postcode.isEmpty()) {

            AddressBuilder addressBuilder = new AddressBuilder(practitionerBuilder);
            addressBuilder.addLine(line1.getString(), line1);
            addressBuilder.addLine(line2.getString(), line2);
            addressBuilder.addLine(line3.getString(), line3);
            addressBuilder.addLine(line4.getString(), line4);
            addressBuilder.setCity(city.getString(), city);
            addressBuilder.setPostcode(postcode.getString(), postcode);
        }

        //clear down any telecoms already on there
        ContactPointBuilder.removeExistingContactPoints(practitionerBuilder);

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

        CsvCell gmpCode = parser.getGmpCode();
        if (!gmpCode.isEmpty()) {
            //only remove GMP idenfifiers, so we don't lose anything added by the ADT feed
            IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);
            identifierBuilder.setValue(gmpCode.getString(), gmpCode);
        }

        CsvCell consultantNHSCode = parser.getConsultantNHSCode();
        if (!consultantNHSCode.isEmpty()) {
            //only remove this specific idenfifier type, so we don't lose anything added by the ADT feed
            IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CONSULTANT_CODE);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CONSULTANT_CODE);
            identifierBuilder.setValue(consultantNHSCode.getString(), consultantNHSCode);
        }

        boolean mapIds = !practitionerBuilder.isIdMapped();
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), mapIds, practitionerBuilder);
    }


}
