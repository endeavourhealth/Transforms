package org.endeavourhealth.transform.vision.transforms;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Practice;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PracticeTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PracticeTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Practice.class);
        while (parser.nextRecord()) {

            try {
                createResource((Practice)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
     }


    private static void createResource(Practice parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper) throws Exception {
        //first up, create the organisation resource
        createOrganisationResource(parser, fhirResourceFiler, csvHelper);

        //then the location and link the two
        createLocationResource(parser, fhirResourceFiler, csvHelper);
    }

    private static void createLocationResource(Practice parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper) throws Exception {

        org.hl7.fhir.instance.model.Location fhirLocation = new org.hl7.fhir.instance.model.Location();
        fhirLocation.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_LOCATION));

        //set the Location ID to that of the Organisation ID
        String organisationID = parser.getOrganisationID();
        fhirLocation.setId(organisationID);

        String houseNameFlat = parser.getAddress1();
        String numberAndStreet = parser.getAddress2();
        String village = parser.getAddress3();
        String town = parser.getAddress4();
        String county = parser.getAddress5();
        String postcode = parser.getPostCode();

        Address fhirAddress = AddressConverter.createAddress(Address.AddressUse.WORK, houseNameFlat, numberAndStreet, village, town, county, postcode);
        fhirLocation.setAddress(fhirAddress);

        String phoneNumber = parser.getPhone();
        ContactPoint fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE, ContactPoint.ContactPointUse.WORK, phoneNumber);
        fhirLocation.addTelecom(fhirContact);

        String faxNumber = parser.getFax();
        fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.FAX, ContactPoint.ContactPointUse.WORK, faxNumber);
        fhirLocation.addTelecom(fhirContact);

        String email = parser.getEmail();
        fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.EMAIL, ContactPoint.ContactPointUse.WORK, email);
        fhirLocation.addTelecom(fhirContact);

        // the location name is the organisation name, that's all we have
        String name = parser.getOrganisationName();
        fhirLocation.setName(name);

        //set the managing organisation for the location, basically itself!
        fhirLocation.setManagingOrganization(csvHelper.createOrganisationReference(organisationID));

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), fhirLocation);
    }

    private static void createOrganisationResource(Practice parser,
                                                   FhirResourceFiler fhirResourceFiler,
                                                   VisionCsvHelper csvHelper) throws Exception {
        Organization fhirOrganisation = new Organization();
        fhirOrganisation.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ORGANIZATION));

        String orgID = parser.getOrganisationID();
        fhirOrganisation.setId(orgID);

        String odsCode = parser.getIdentifier();
        Identifier fhirIdentifier = IdentifierHelper.createOdsOrganisationIdentifier(odsCode);
        fhirOrganisation.addIdentifier(fhirIdentifier);

        String name = parser.getOrganisationName();
        fhirOrganisation.setName(name);

        //try to get a org type from the name, i.e. Tinshill Surgery = GP_PRACTICE
        OrganisationType fhirOrgType = convertOrganisationType(name);
        if (fhirOrgType != null) {
            fhirOrganisation.setType(CodeableConceptHelper.createCodeableConcept(fhirOrgType));
        } else {
            //if the org type can't be mapped to one of the value set, store as a freetext type
            LOG.info("Cannot map organisation name: " + name);
            fhirOrganisation.setType(CodeableConceptHelper.createCodeableConcept(name));
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), fhirOrganisation);
    }

    private static OrganisationType convertOrganisationType(String csvOrganisationName) {
        //the below mappings are based on what was present in the Vision CSV sample files
        csvOrganisationName = csvOrganisationName.toLowerCase();

        if (csvOrganisationName.contains("practice")
                || csvOrganisationName.contains("surgery")) {
            return OrganisationType.GP_PRACTICE;
        } else if (csvOrganisationName.contains("ccg")) {
            return OrganisationType.CCG;
        } else if (csvOrganisationName.contains("pct")
                || csvOrganisationName.contains("primary care trust")) {
            return OrganisationType.PCT;
        } else if (csvOrganisationName.contains("hospital")
                || csvOrganisationName.contains("nhs trust")) {
            return OrganisationType.NHS_TRUST;

        } else {
            return null;
        }
    }
}