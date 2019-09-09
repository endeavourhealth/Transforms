package org.endeavourhealth.transform.tpp.csv.transforms.admin;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.csv.helpers.EmisAdminCacheFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.admin.SRCcg;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRCcgTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRCcgTransformer.class);

    public static final String CCG_KEY_PREFIX = "CCG-";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRCcg.class);
        if (parser != null) {
            EmisAdminCacheFiler adminCacheFiler = new EmisAdminCacheFiler(TppCsvHelper.ADMIN_CACHE_KEY);

            while (parser.nextRecord()) {

                try {
                    createResource((SRCcg) parser, fhirResourceFiler, csvHelper, adminCacheFiler);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }

            adminCacheFiler.close();
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(SRCcg parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper,
                                      EmisAdminCacheFiler adminCacheFiler) throws Exception {

        //first up, create the organisation resource
        createOrganisationResource(parser, fhirResourceFiler, csvHelper, adminCacheFiler);

        //then the location and link the two
        createLocationResource(parser, fhirResourceFiler, csvHelper, adminCacheFiler);

    }

    public static void createLocationResource(SRCcg parser,
                                              FhirResourceFiler fhirResourceFiler,
                                              TppCsvHelper csvHelper,
                                              EmisAdminCacheFiler adminCacheFiler) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();

        LocationBuilder locationBuilder = new LocationBuilder();
        locationBuilder.setId(CCG_KEY_PREFIX + rowIdCell.getString(), rowIdCell);

        //removed data column wasn't present prior to v88
        CsvCell removedData = parser.getRemovedData(); //renamed to avoid confusion. Obsolete != delete
        if (removedData != null //note this cell wasn't present in all versions, so need to check for null cell
                && removedData.getBoolean()) {

            adminCacheFiler.deleteAdminResourceFromCache(locationBuilder);

            locationBuilder.setDeletedAudit(removedData);
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), locationBuilder);
            return;
        }

        CsvCell nameCell = parser.getName();
        if (!nameCell.isEmpty()) {
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
        CsvCell nameOfBuildingCell = parser.getHouseName();
        if (!nameOfBuildingCell.isEmpty()) {
            addressBuilder.addLine(nameOfBuildingCell.getString(), nameOfBuildingCell);
        }
        CsvCell numberOfBuildingCell = parser.getHouseNumber();
        CsvCell nameOfRoadCell = parser.getNameOfRoad();
        addressBuilder.addLineFromHouseNumberAndRoad(numberOfBuildingCell, nameOfRoadCell);

        CsvCell nameOfLocalityCell = parser.getNameOfLocality();
        if (!nameOfLocalityCell.isEmpty()) {
            addressBuilder.addLine(nameOfLocalityCell.getString(), nameOfLocalityCell);
        }
        CsvCell nameOfTownCell = parser.getNameOfTown();
        if (!nameOfTownCell.isEmpty()) {
            addressBuilder.setCity(nameOfTownCell.getString(), nameOfTownCell);
        }
        CsvCell nameOfCountyCell = parser.getNameOfCounty();
        if (!nameOfCountyCell.isEmpty()) {
            addressBuilder.setDistrict(nameOfCountyCell.getString(), nameOfCountyCell);
        }

        CsvCell fullPostCodeCell = parser.getFullPostCode();
        if (!fullPostCodeCell.isEmpty()) {
            addressBuilder.setPostcode(fullPostCodeCell.getString(), fullPostCodeCell);
        }

        CsvCell contactNumberCell = parser.getTelephone();
        if (!contactNumberCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.PHONE, contactNumberCell, locationBuilder);
        }

        CsvCell secondaryContactCell = parser.getSecondaryTelephone();
        if (!secondaryContactCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.PHONE, secondaryContactCell, locationBuilder);
        }

        CsvCell faxCell = parser.getFax();
        if (!faxCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.FAX, faxCell, locationBuilder);
        }

        //set the managing organisation for the location, basically itself!
        Reference organisationReference = ReferenceHelper.createReference(ResourceType.Organization, CCG_KEY_PREFIX + rowIdCell.getString());
        locationBuilder.setManagingOrganisation(organisationReference, rowIdCell);

        //save to admin cache too
        adminCacheFiler.saveAdminResourceToCache(locationBuilder);

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), locationBuilder);
    }

    public static void createOrganisationResource(SRCcg parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  TppCsvHelper csvHelper,
                                                  EmisAdminCacheFiler adminCacheFiler) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();

        OrganizationBuilder organizationBuilder = new OrganizationBuilder();
        organizationBuilder.setId(CCG_KEY_PREFIX + rowIdCell.getString(), rowIdCell);

        CsvCell obsoleteCell = parser.getRemovedData();
        if (obsoleteCell != null && obsoleteCell.getBoolean()) {

            adminCacheFiler.deleteAdminResourceFromCache(organizationBuilder);

            organizationBuilder.setDeletedAudit(obsoleteCell);
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
        addressBuilder.setUse(Address.AddressUse.HOME);
        CsvCell nameOfBuildingCell = parser.getHouseName();
        if (!nameOfBuildingCell.isEmpty()) {
            addressBuilder.addLine(nameOfBuildingCell.getString(), nameOfBuildingCell);
        }
        CsvCell numberOfBuildingCell = parser.getHouseNumber();
        CsvCell nameOfRoadCell = parser.getNameOfRoad();
        addressBuilder.addLineFromHouseNumberAndRoad(numberOfBuildingCell, nameOfRoadCell);

        CsvCell nameOfLocalityCell = parser.getNameOfLocality();
        if (!nameOfLocalityCell.isEmpty()) {
            addressBuilder.addLine(nameOfLocalityCell.getString(), nameOfLocalityCell);
        }
        CsvCell nameOfTownCell = parser.getNameOfTown();
        if (!nameOfTownCell.isEmpty()) {
            addressBuilder.setCity(nameOfTownCell.getString(), nameOfTownCell);
        }
        CsvCell nameOfCountyCell = parser.getNameOfCounty();
        if (!nameOfCountyCell.isEmpty()) {
            addressBuilder.setDistrict(nameOfCountyCell.getString(), nameOfCountyCell);
        }

        CsvCell fullPostCodeCell = parser.getFullPostCode();
        if (!fullPostCodeCell.isEmpty()) {
            addressBuilder.setPostcode(fullPostCodeCell.getString(), fullPostCodeCell);
        }

        CsvCell contactNumberCell = parser.getTelephone();
        if (!contactNumberCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.PHONE, contactNumberCell, organizationBuilder);
        }

        CsvCell secondaryContactCell = parser.getSecondaryTelephone();
        if (!secondaryContactCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.PHONE, secondaryContactCell, organizationBuilder);
        }

        CsvCell faxCell = parser.getFax();
        if (!faxCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.FAX, faxCell, organizationBuilder);
        }

        Reference locationReference = ReferenceHelper.createReference(ResourceType.Location, CCG_KEY_PREFIX + rowIdCell.getString()); //we use the ID as the source both the org and location
        organizationBuilder.setMainLocation(locationReference);

        //save to admin cache too
        adminCacheFiler.saveAdminResourceToCache(organizationBuilder);

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);
    }

    private static void createContactPoint(ContactPoint.ContactPointSystem system, CsvCell contactCell, HasContactPointI parentBuilder) throws Exception {

        ContactPointBuilder contactPointBuilder = new ContactPointBuilder(parentBuilder);
        contactPointBuilder.setSystem(system);
        contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
        contactPointBuilder.setValue(contactCell.getString(), contactCell);
    }
}
