package org.endeavourhealth.transform.vision.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.ContactPointHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.IdentifierHelper;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Practice;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

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
        //first up, create the location resource
        createLocationResource(parser, fhirResourceFiler, csvHelper);

        //then the organisation and link
        createOrganisationResource(parser, fhirResourceFiler, csvHelper);
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

//        String organisationType = parser.getOrganisationType();
//        if (!Strings.isNullOrEmpty(organisationType)) {
//            OrganisationType fhirOrgType = convertOrganisationType(organisationType);
//            if (fhirOrgType != null) {
//                fhirOrganisation.setType(CodeableConceptHelper.createCodeableConcept(fhirOrgType));
//            } else {
//                //if the org type from the CSV can't be mapped to one of the value set, store as a freetext type
//                //LOG.info("Unmapped organisation type " + organisationType);
//                fhirOrganisation.setType(CodeableConceptHelper.createCodeableConcept(organisationType));
//            }
//        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), fhirOrganisation);

        //this resource exists in our admin resource cache, so we can populate the
        //main database when new practices come on, so we need to update that too
        csvHelper.saveAdminResourceToCache(fhirOrganisation);
    }

    private static void createLocationResource(Practice parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper) throws Exception {

        org.hl7.fhir.instance.model.Location fhirLocation = new org.hl7.fhir.instance.model.Location();
        fhirLocation.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_LOCATION));

        String locationGuid = UUID.randomUUID().toString();  //create a new identifier to link to the org
        fhirLocation.setId(locationGuid);

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

//        Date openDate = parser.getOpenDate();
//        Date closeDate = parser.getCloseDate();
//        boolean deleted = parser.getDeleted();
//        Period fhirPeriod = PeriodHelper.createPeriod(openDate, closeDate);
//        if (PeriodHelper.isActive(fhirPeriod) && !deleted) {
//            fhirLocation.setStatus(org.hl7.fhir.instance.model.Location.LocationStatus.ACTIVE);
//        } else {
//            fhirLocation.setStatus(org.hl7.fhir.instance.model.Location.LocationStatus.INACTIVE);
//        }
//        fhirLocation.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.ACTIVE_PERIOD, fhirPeriod));

//        String mainContactName = parser.getMainContactName();
//        if (!Strings.isNullOrEmpty(mainContactName)) {
//            fhirLocation.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.LOCATION_MAIN_CONTACT, new StringType(mainContactName)));
//        }

        String name = parser.getOrganisationName();   // the location name is the organisation name, that's all we have
        fhirLocation.setName(name);

//        String type = parser.getLocationTypeDescription();
//        fhirLocation.setType(CodeableConceptHelper.createCodeableConcept(type));

        String organisationID = parser.getOrganisationID();
        fhirLocation.setManagingOrganization(csvHelper.createOrganisationReference(organisationID));

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), fhirLocation);

        //this resource exists in our admin resource cache, so we can populate the
        //main database when new practices come on, so we need to update that too
        csvHelper.saveAdminResourceToCache(fhirLocation);
    }

    private static OrganisationType convertOrganisationType(String csvOrganisationType) {
        try {
            return OrganisationType.fromDescription(csvOrganisationType);
        } catch (Exception ex) {

            //the below mappings are based on what was present in the EMIS CSV sample files
            //EMIS has been asked for a complete list, but until this is made available, these
            //are the only known types. There are a number of organisation types, such as "Hospice"
            //or "Community" which don't map to any official NHS organisation type
            if (csvOrganisationType.equalsIgnoreCase("General Practice")
                    || csvOrganisationType.equalsIgnoreCase("General Practice Surgery")
                    || csvOrganisationType.equalsIgnoreCase("Main Surgery")) {
                return OrganisationType.GP_PRACTICE;

            } else if (csvOrganisationType.equalsIgnoreCase("CCG")) {
                return OrganisationType.CCG;

            } else if (csvOrganisationType.equalsIgnoreCase("PCT Site")
                    || csvOrganisationType.equalsIgnoreCase("Primary Care Trust")) {
                return OrganisationType.PCT;

            } else if (csvOrganisationType.equalsIgnoreCase("Hospital")
                    || csvOrganisationType.equalsIgnoreCase("NHS Trust Site")
                    || csvOrganisationType.equalsIgnoreCase("NHS Trust")) {
                return OrganisationType.NHS_TRUST;

            } else {
                return null;
            }
        }

    }
}