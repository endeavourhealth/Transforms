package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ContactPointHelper;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.common.ods.OdsWebService;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.subscriber.transforms.OrganisationTransformer;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Practice;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PracticeTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PracticeTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
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


    private static void createOrganisationResource(Practice parser,
                                                   FhirResourceFiler fhirResourceFiler,
                                                   VisionCsvHelper csvHelper) throws Exception {

        OrganizationBuilder organizationBuilder = new OrganizationBuilder();
        CsvCell orgIdCell = parser.getOrganisationID();
        organizationBuilder.setId(orgIdCell.getString(), orgIdCell);

        CsvCell odsCodeCell = parser.getIdentifier();
        if (!odsCodeCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
            identifierBuilder.setValue(odsCodeCell.getString(), odsCodeCell);
        }

        //SD-201 phone and address were simply missed on the Org resource
        transformAddress(parser, organizationBuilder);
        transformPhoneNumbers(parser, organizationBuilder);

        CsvCell nameCell = parser.getOrganisationName();
        if (!nameCell.isEmpty()) {
            organizationBuilder.setName(nameCell.getString(), nameCell);
        }

        //link to the FHIR Location we'll create, which uses the same source ID
        Reference locationReference = ReferenceHelper.createReference(ResourceType.Location, orgIdCell.getString());
        organizationBuilder.setMainLocation(locationReference, orgIdCell);

        //SD-201 we don't get type or parent or anything similar from Vision, so use the ODS API to look up what we can
        if (!odsCodeCell.isEmpty()) {
            String odsCode = odsCodeCell.getString();
            OdsOrganisation odsRecord = OdsWebService.lookupOrganisationViaRest(odsCode);
            if (odsRecord != null) {

                OrganisationType fhirOrgType = OrganisationTransformer.findOdsOrganisationType(odsRecord);
                if (fhirOrgType != null) {
                    organizationBuilder.setType(fhirOrgType);
                }

                String parentId = populateParentFromOds(odsRecord, fhirResourceFiler);
                if (parentId != null) {
                    Reference parentReference = VisionCsvHelper.createOrganisationReference(parentId);
                    organizationBuilder.setParentOrganisation(parentReference);
                }
            }
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);
    }

    private static String populateParentFromOds(OdsOrganisation childRecord, FhirResourceFiler fhirResourceFiler) throws Exception {

        OdsOrganisation parent = OrganisationTransformer.findParentOrganisation(childRecord);
        if (parent == null) {
            return null;
        }

        OrganizationBuilder organizationBuilder = new OrganizationBuilder();

        //use the org ID as the unique identifier for the FHIR org
        String orgId = parent.getOdsCode();
        if (Strings.isNullOrEmpty(orgId)) {
            return null;
        }
        organizationBuilder.setId(orgId);

        String name = parent.getOrganisationName();
        organizationBuilder.setName(name);

        OrganisationType fhirOrgType = OrganisationTransformer.findOdsOrganisationType(parent);
        if (fhirOrgType != null) {
            organizationBuilder.setType(fhirOrgType);
        }

        AddressBuilder addressBuilder = new AddressBuilder(organizationBuilder);

        String line1 = parent.getAddressLine1();
        addressBuilder.addLine(line1);

        String line2 = parent.getAddressLine2();
        addressBuilder.addLine(line2);

        String town = parent.getTown();
        addressBuilder.setCity(town);

        String county = parent.getCounty();
        addressBuilder.setDistrict(county);

        String postcode = parent.getPostcode();
        addressBuilder.setPostcode(postcode);

        //recurse to generate the parent of this parent
        String parentId = populateParentFromOds(parent, fhirResourceFiler);
        if (parentId != null) {
            Reference parentReference = VisionCsvHelper.createOrganisationReference(parentId);
            organizationBuilder.setParentOrganisation(parentReference);
        }

        //save
        fhirResourceFiler.saveAdminResource(null, organizationBuilder);

        //and return the org ID so it can be set on the child resource
        return orgId;
    }


    private static void createLocationResource(Practice parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper) throws Exception {

        LocationBuilder locationBuilder = new LocationBuilder();

        //set the Location ID to that of the Organisation ID
        CsvCell organisationIdCell = parser.getOrganisationID();
        locationBuilder.setId(organisationIdCell.getString(), organisationIdCell);

        transformAddress(parser, locationBuilder);
        transformPhoneNumbers(parser, locationBuilder);

        // the location name is the organisation name, that's all we have
        CsvCell nameCell = parser.getOrganisationName();
        if (!nameCell.isEmpty()) {
            locationBuilder.setName(nameCell.getString(), nameCell);
        }

        //set the managing organisation for the location, basically itself!
        Reference organisationReference = csvHelper.createOrganisationReference(organisationIdCell.getString());
        locationBuilder.setManagingOrganisation(organisationReference, organisationIdCell);

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), locationBuilder);
    }

    private static void transformPhoneNumbers(Practice parser, HasContactPointI parentBuilder) {

        CsvCell phoneNumberCell = parser.getPhone();
        if (!phoneNumberCell.isEmpty()) {
            String value = phoneNumberCell.getString();

            ContactPointBuilder builder = new ContactPointBuilder(parentBuilder);
            builder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            builder.setUse(ContactPoint.ContactPointUse.WORK);
            builder.setValue(value, phoneNumberCell);
        }

        CsvCell faxNumberCell = parser.getFax();
        if (!faxNumberCell.isEmpty()) {
            String value = faxNumberCell.getString();

            ContactPointBuilder builder = new ContactPointBuilder(parentBuilder);
            builder.setSystem(ContactPoint.ContactPointSystem.FAX);
            builder.setUse(ContactPoint.ContactPointUse.WORK);
            builder.setValue(value, faxNumberCell);
        }

        CsvCell emailCell = parser.getEmail();
        if (!emailCell.isEmpty()) {
            String value = emailCell.getString();

            ContactPointBuilder builder = new ContactPointBuilder(parentBuilder);
            builder.setSystem(ContactPoint.ContactPointSystem.EMAIL);
            builder.setUse(ContactPoint.ContactPointUse.WORK);
            builder.setValue(value, emailCell);
        }

    }

    private static void transformAddress(Practice parser, HasAddressI parentBuilder) {

        CsvCell houseNameFlat = parser.getAddress1();
        CsvCell numberAndStreet = parser.getAddress2();
        CsvCell village = parser.getAddress3();
        CsvCell town = parser.getAddress4();
        CsvCell county = parser.getAddress5();
        CsvCell postcode = parser.getPostCode();

        AddressBuilder addressBuilder = new AddressBuilder(parentBuilder);
        addressBuilder.setUse(Address.AddressUse.WORK);
        addressBuilder.addLine(houseNameFlat.getString(), houseNameFlat);
        addressBuilder.addLine(numberAndStreet.getString(), numberAndStreet);
        addressBuilder.addLine(village.getString(), village);
        addressBuilder.setCity(town.getString(), town);
        addressBuilder.setDistrict(county.getString(), county);
        addressBuilder.setPostcode(postcode.getString(), postcode);
    }


    /*private static OrganisationType convertOrganisationType(String csvOrganisationName) {
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
    }*/
}