package org.endeavourhealth.transform.tpp.csv.transforms.admin;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.csv.helpers.EmisAdminCacheFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.admin.SROrganisation;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SROrganisationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SROrganisationTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SROrganisation.class);
        if (parser != null) {
            EmisAdminCacheFiler adminCacheFiler = new EmisAdminCacheFiler(TppCsvHelper.ADMIN_CACHE_KEY);

            while (parser.nextRecord()) {

                try {
                    createOrganisationResource((SROrganisation)parser, fhirResourceFiler, csvHelper, adminCacheFiler);
                    createLocationResource((SROrganisation)parser, fhirResourceFiler, csvHelper, adminCacheFiler);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }

            adminCacheFiler.close();
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createOrganisationResource(SROrganisation parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  TppCsvHelper csvHelper,
                                                  EmisAdminCacheFiler adminCacheFiler) throws Exception {

        //note that throughout the TPP files, the organisation ID (i.e. ODS code) is used rather than the rowIdentifier when referring to orgs
        CsvCell idCell = parser.getID();

        if (idCell.isEmpty()) {
            //no point logging this tens of thousands of times if no action will be taken
            //TransformWarnings.log(LOG, parser, "Skipping organisation RowIdentifier {} because no ID present", parser.getRowIdentifier());
            return;
        }

        OrganizationBuilder organizationBuilder = new OrganizationBuilder();
        organizationBuilder.setId(idCell.getString(), idCell);

        CsvCell deletedCell = parser.getRemovedData();
        if (deletedCell != null && deletedCell.getIntAsBoolean()) {
            adminCacheFiler.deleteAdminResourceFromCache(organizationBuilder);

            organizationBuilder.setDeletedAudit(deletedCell);
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), organizationBuilder);
            return;
        }

        CsvCell obsoleteCell = parser.getMadeObsolete();
        boolean obsolete = false;
        if (obsoleteCell != null && obsoleteCell.getBoolean()) {
            obsolete = true;
        }

        CsvCell nameCell = parser.getName();
        if (!nameCell.isEmpty()) {
            if (!obsolete) {
                organizationBuilder.setName(nameCell.getString(), nameCell);
            } else {
                organizationBuilder.setName(nameCell.getString() + "(Obsolete)", nameCell);
            }
        } else {
            organizationBuilder.setName("(Obsolete)", nameCell);
        }

        organizationBuilder.setOdsCode(idCell.getString(), idCell);

        AddressBuilder addressBuilder = new AddressBuilder(organizationBuilder);
        addressBuilder.setUse(Address.AddressUse.WORK);

        CsvCell nameOfBuildingCell = parser.getHouseName();
        if (!nameOfBuildingCell.isEmpty()) {
            addressBuilder.addLine(nameOfBuildingCell.getString(), nameOfBuildingCell);
        }

        // Some addresses have a house name with or without a street number or road name Try to handle combinations
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

        CsvCell ccgCell = parser.getIDCcg();
        if (!ccgCell.isEmpty() && ccgCell.getInt() >= 0) {
            String localCcgId = SRCcgTransformer.CCG_KEY_PREFIX + ccgCell.getString();
            Reference ccgReference = ReferenceHelper.createReference(ResourceType.Organization, localCcgId);
            organizationBuilder.setParentOrganisation(ccgReference, ccgCell);

        } else {
            //only transform the trust cell if no CCG cell is present. If CCG is present, this is the true parent
            //organisation. Only if not present, should the trust be used (for GP practices this points to obsolete PCTs)
            CsvCell trustCell = parser.getIDTrust();
            if (!trustCell.isEmpty() && trustCell.getInt() >= 0) {
                String localTrustId = SRTrustTransformer.TRUST_KEY_PREFIX + trustCell.getString();
                Reference trustReference = ReferenceHelper.createReference(ResourceType.Organization, localTrustId);
                organizationBuilder.setParentOrganisation(trustReference, trustCell);
            }
        }

        Reference locationReference = ReferenceHelper.createReference(ResourceType.Location, idCell.getString()); //we use the ID as the source both the org and location
        organizationBuilder.setMainLocation(locationReference);

        adminCacheFiler.saveAdminResourceToCache(organizationBuilder);
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);
    }


    public static void createLocationResource(SROrganisation parser,
                                              FhirResourceFiler fhirResourceFiler,
                                              TppCsvHelper csvHelper,
                                              EmisAdminCacheFiler adminCacheFiler) throws Exception {

        //note that throughout the TPP files, the organisation ID (i.e. ODS code) is used rather than the rowIdentifier when referring to orgs
        CsvCell idCell = parser.getID();

        if (idCell.isEmpty()) {
            //already have logged this warning when creating the Organisation resource
            //TransformWarnings.log(LOG, parser, "Skipping organisation RowIdentifier {} because no ID present", parser.getRowIdentifier());
            return;
        }

        LocationBuilder locationBuilder = new LocationBuilder();
        locationBuilder.setId(idCell.getString(), idCell);

        CsvCell deletedCell = parser.getRemovedData();
        if (deletedCell != null && deletedCell.getIntAsBoolean()) {
            adminCacheFiler.deleteAdminResourceFromCache(locationBuilder);

            locationBuilder.setDeletedAudit(deletedCell);
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), locationBuilder);
            return;
        }

        CsvCell obsoleteCell = parser.getMadeObsolete();
        if (obsoleteCell != null && obsoleteCell.getBoolean()) {
            locationBuilder.setStatus(Location.LocationStatus.INACTIVE);
        } else {
            locationBuilder.setStatus(Location.LocationStatus.ACTIVE);
        }

        CsvCell nameCell = parser.getName();
        if (!nameCell.getString().isEmpty()) {
            locationBuilder.setName(nameCell.getString(), nameCell);
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

        Reference organisationReference = ReferenceHelper.createReference(ResourceType.Organization, idCell.getString()); //we use the ID as the source both the org and location
        locationBuilder.setManagingOrganisation(organisationReference);

        adminCacheFiler.saveAdminResourceToCache(locationBuilder);
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), locationBuilder);
    }

    private static void createContactPoint(ContactPoint.ContactPointSystem system, CsvCell contactCell, HasContactPointI parentBuilder) {

        ContactPointBuilder contactPointBuilder = new ContactPointBuilder(parentBuilder);
        contactPointBuilder.setSystem(system);
        contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
        contactPointBuilder.setValue(contactCell.getString(), contactCell);
    }

}
