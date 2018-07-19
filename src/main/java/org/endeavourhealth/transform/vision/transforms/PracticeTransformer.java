package org.endeavourhealth.transform.vision.transforms;

import org.endeavourhealth.common.fhir.ContactPointHelper;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Practice;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
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

        //if the Practice file has skipped a day then it might not be available
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((Practice) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
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

        LocationBuilder locationBuilder = new LocationBuilder();

        //set the Location ID to that of the Organisation ID
        CsvCell organisationID = parser.getOrganisationID();
        locationBuilder.setId(organisationID.getString(), organisationID);

        CsvCell houseNameFlat = parser.getAddress1();
        CsvCell numberAndStreet = parser.getAddress2();
        CsvCell village = parser.getAddress3();
        CsvCell town = parser.getAddress4();
        CsvCell county = parser.getAddress5();
        CsvCell postcode = parser.getPostCode();

        AddressBuilder addressBuilder = new AddressBuilder(locationBuilder);
        addressBuilder.setUse(Address.AddressUse.WORK);
        addressBuilder.addLine(houseNameFlat.getString(), houseNameFlat);
        addressBuilder.addLine(numberAndStreet.getString(), numberAndStreet);
        addressBuilder.addLine(village.getString(), village);
        addressBuilder.setTown(town.getString(), town);
        addressBuilder.setDistrict(county.getString(), county);
        addressBuilder.setPostcode(postcode.getString(), postcode);

        CsvCell phoneNumber = parser.getPhone();
        if (!phoneNumber.isEmpty()) {
            ContactPoint fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE, ContactPoint.ContactPointUse.WORK, phoneNumber.getString());
            locationBuilder.addTelecom(fhirContact, phoneNumber);
        }

        CsvCell faxNumber = parser.getFax();
        if (!faxNumber.isEmpty()) {
            ContactPoint fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.FAX, ContactPoint.ContactPointUse.WORK, faxNumber.getString());
            locationBuilder.addTelecom(fhirContact, faxNumber);
        }

        CsvCell email = parser.getEmail();
        if (!email.isEmpty()) {
            ContactPoint fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.EMAIL, ContactPoint.ContactPointUse.WORK, email.getString());
            locationBuilder.addTelecom(fhirContact, email);
        }

        // the location name is the organisation name, that's all we have
        CsvCell name = parser.getOrganisationName();
        if (!name.isEmpty()) {
            locationBuilder.setName(name.getString(), name);
        }

        //set the managing organisation for the location, basically itself!
        Reference organisationReference = csvHelper.createOrganisationReference(organisationID.getString());
        locationBuilder.setManagingOrganisation(organisationReference, organisationID);

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), locationBuilder);
    }

    private static void createOrganisationResource(Practice parser,
                                                   FhirResourceFiler fhirResourceFiler,
                                                   VisionCsvHelper csvHelper) throws Exception {

        OrganizationBuilder organizationBuilder = new OrganizationBuilder();
        CsvCell orgID = parser.getOrganisationID();
        organizationBuilder.setId(orgID.getString(), orgID);

        CsvCell odsCode = parser.getIdentifier();
        if (!odsCode.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
            identifierBuilder.setValue(odsCode.getString(), odsCode);
        }

        CsvCell name = parser.getOrganisationName();
        if (!name.isEmpty()) {
            organizationBuilder.setName(name.getString(), name);
        }

        //try to get a org type from the name, i.e. Tinshill Surgery = GP_PRACTICE
        if (!name.isEmpty()) {
            OrganisationType fhirOrgType = convertOrganisationType(name.getString());
            if (fhirOrgType != null) {
                organizationBuilder.setType(fhirOrgType);
            } else {
                //if the org type from the CSV can't be mapped to one of the value set, store as a freetext type
                organizationBuilder.setTypeFreeText(name.getString(), name);
            }
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);
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