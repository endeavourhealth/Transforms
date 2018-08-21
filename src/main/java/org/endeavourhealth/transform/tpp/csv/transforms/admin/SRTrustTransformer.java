package org.endeavourhealth.transform.tpp.csv.transforms.admin;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.csv.helpers.EmisAdminCacheFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.admin.SRTrust;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRTrustTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRTrustTransformer.class);

    public static final String TRUST_KEY_PREFIX = "TRUST-";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRTrust.class);
        if (parser != null) {
            while (parser.nextRecord()) {
                EmisAdminCacheFiler adminCacheFiler = new EmisAdminCacheFiler(TppCsvHelper.ADMIN_CACHE_KEY);

                try {
                    createResource((SRTrust) parser, fhirResourceFiler, csvHelper, adminCacheFiler);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }

                adminCacheFiler.close();
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(SRTrust parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper,
                                      EmisAdminCacheFiler adminCacheFiler) throws Exception {

        //first up, create the organisation resource
        createOrganisationResource(parser, fhirResourceFiler, csvHelper, adminCacheFiler);

        //then the location and link the two
        createLocationResource(parser, fhirResourceFiler, csvHelper, adminCacheFiler);

    }

    public static void createLocationResource(SRTrust parser,
                                              FhirResourceFiler fhirResourceFiler,
                                              TppCsvHelper csvHelper,
                                              EmisAdminCacheFiler adminCacheFiler) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();

        LocationBuilder locationBuilder = new LocationBuilder();
        locationBuilder.setId(TRUST_KEY_PREFIX + rowIdCell.getString(), rowIdCell);

        CsvCell obsoleteCell  = parser.getRemovedData();

        if (obsoleteCell != null && !obsoleteCell.isEmpty() && obsoleteCell.getBoolean() ) {
            adminCacheFiler.deleteAdminResourceFromCache(locationBuilder);
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), locationBuilder);
            return;
        }

        CsvCell nameCell = parser.getName();
        if (!nameCell.getString().isEmpty()) {
            locationBuilder.setName(nameCell.getString(), nameCell);
        }

        CsvCell odsCell = parser.getOdsCode();
        if (!odsCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(locationBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setValue(odsCell.getString(), odsCell);
        }

        AddressBuilder addressBuilder = new AddressBuilder(locationBuilder);
        addressBuilder.setUse(Address.AddressUse.WORK);
        CsvCell nameOfBuildingCell  = parser.getHouseName();
        if (!nameOfBuildingCell.isEmpty()) {
            addressBuilder.addLine(nameOfBuildingCell.getString(), nameOfBuildingCell);
        }
        CsvCell numberOfBuildingCell = parser.getHouseNumber();
        CsvCell nameOfRoadCell = parser.getNameOfRoad();
        addressBuilder.addLineFromHouseNumberAndRoad(numberOfBuildingCell, nameOfRoadCell);

        CsvCell nameOfLocalityCell  = parser.getNameOfLocality();
        if (!nameOfLocalityCell.isEmpty()) {
            addressBuilder.addLine(nameOfLocalityCell.getString(), nameOfLocalityCell);
        }
        CsvCell nameOfTownCell  = parser.getNameOfTown();
        if (!nameOfTownCell.isEmpty()) {
            addressBuilder.setTown(nameOfTownCell.getString(), nameOfTownCell);
        }
        CsvCell nameOfCountyCell  = parser.getNameOfCounty();
        if (!nameOfCountyCell.isEmpty()) {
            addressBuilder.setDistrict(nameOfCountyCell.getString(), nameOfCountyCell);
        }

        CsvCell fullPostCodeCell  = parser.getFullPostCode();
        if (!fullPostCodeCell.isEmpty()) {
            addressBuilder.setPostcode(fullPostCodeCell.getString(), fullPostCodeCell);
        }

        CsvCell contactNumberCell = parser.getTelephone();
        if (!contactNumberCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.PHONE, contactNumberCell, rowIdCell, locationBuilder);
        }

        CsvCell secondaryContactCell = parser.getSecondaryTelephone();
        if (!secondaryContactCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.PHONE, secondaryContactCell, rowIdCell, locationBuilder);
        }

        CsvCell faxCell = parser.getFax();
        if (!faxCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.FAX, faxCell, rowIdCell, locationBuilder);
        }

        //set the managing organisation for the location, basically itself!
        Reference organisationReference = ReferenceHelper.createReference(ResourceType.Organization, TRUST_KEY_PREFIX + rowIdCell.getString());
        locationBuilder.setManagingOrganisation(organisationReference, rowIdCell);

        adminCacheFiler.saveAdminResourceToCache(locationBuilder);
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), locationBuilder);
    }

    public static void createOrganisationResource(SRTrust parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  TppCsvHelper csvHelper,
                                                  EmisAdminCacheFiler adminCacheFiler) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();

        OrganizationBuilder organizationBuilder = new OrganizationBuilder();
        organizationBuilder.setId(TRUST_KEY_PREFIX + rowIdCell.getString());

        CsvCell deleted = parser.getRemovedData();
        if (deleted != null &&!deleted.isEmpty() && deleted.getBoolean()) {
            adminCacheFiler.deleteAdminResourceFromCache(organizationBuilder);
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), organizationBuilder);
            return;
        }

        CsvCell nameCell = parser.getName();
        if (!nameCell.getString().isEmpty()) {
            organizationBuilder.setName(nameCell.getString(), nameCell);
        }

        CsvCell odsCell = parser.getOdsCode();
        if (!odsCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setValue(odsCell.getString(), odsCell);
        }

        AddressBuilder addressBuilder = new AddressBuilder(organizationBuilder);
        addressBuilder.setUse(Address.AddressUse.WORK);
        CsvCell nameOfBuildingCell  = parser.getHouseName();
        if (!nameOfBuildingCell.isEmpty()) {
            addressBuilder.addLine(nameOfBuildingCell.getString(), nameOfBuildingCell);
        }
        CsvCell numberOfBuildingCell = parser.getHouseNumber();
        CsvCell nameOfRoadCell = parser.getNameOfRoad();
        addressBuilder.addLineFromHouseNumberAndRoad(numberOfBuildingCell, nameOfRoadCell);

        CsvCell nameOfLocalityCell  = parser.getNameOfLocality();
        if (!nameOfLocalityCell.isEmpty()) {
            addressBuilder.addLine(nameOfLocalityCell.getString(), nameOfLocalityCell);
        }
        CsvCell nameOfTownCell  = parser.getNameOfTown();
        if (!nameOfTownCell.isEmpty()) {
            addressBuilder.setTown(nameOfTownCell.getString(), nameOfTownCell);
        }
        CsvCell nameOfCountyCell  = parser.getNameOfCounty();
        if (!nameOfCountyCell.isEmpty()) {
            addressBuilder.setDistrict(nameOfCountyCell.getString(), nameOfCountyCell);
        }

        CsvCell fullPostCodeCell = parser.getFullPostCode();
        if (!fullPostCodeCell.isEmpty()) {
            addressBuilder.setPostcode(fullPostCodeCell.getString(), fullPostCodeCell);
        }

        CsvCell contactNumberCell = parser.getTelephone();
        if (!contactNumberCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.PHONE, contactNumberCell, rowIdCell, organizationBuilder);
        }

        CsvCell secondaryContactCell = parser.getSecondaryTelephone();
        if (!secondaryContactCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.PHONE, secondaryContactCell, rowIdCell, organizationBuilder);
        }

        CsvCell faxCell = parser.getFax();
        if (!faxCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.FAX, faxCell, rowIdCell, organizationBuilder);
        }

        Reference locationReference = ReferenceHelper.createReference(ResourceType.Location, TRUST_KEY_PREFIX + rowIdCell.getString()); //we use the ID as the source both the org and location
        organizationBuilder.setMainLocation(locationReference);

        adminCacheFiler.saveAdminResourceToCache(organizationBuilder);
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);
    }

    private static void createContactPoint(ContactPoint.ContactPointSystem system, CsvCell contactCell, CsvCell rowIdCell, HasContactPointI parentBuilder) throws Exception {

        ContactPoint.ContactPointUse use = ContactPoint.ContactPointUse.WORK;

        ContactPointBuilder contactPointBuilder = new ContactPointBuilder(parentBuilder);
        contactPointBuilder.setId(rowIdCell.getString(), contactCell);
        contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE, contactCell);
        contactPointBuilder.setUse(use, contactCell);

        contactPointBuilder.setValue(contactCell.getString(), contactCell);
    }
}
