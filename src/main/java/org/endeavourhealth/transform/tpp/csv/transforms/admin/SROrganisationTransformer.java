package org.endeavourhealth.transform.tpp.csv.transforms.admin;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.LocationResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.admin.SROrganisation;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SROrganisationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SROrganisationTransformer.class);
    private static final String TRUST_PREFIX_STRING="TRUST-";
    private static final String CCG_PREFIX_STRING="CCG-";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SROrganisation.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SROrganisation) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(SROrganisation parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        //first up, create the organisation resource
        OrganizationBuilder organizationBuilder = createOrganisationResource(parser, fhirResourceFiler, csvHelper);
        if (organizationBuilder == null) {
            return;
        }

        //then the location and link the two
        LocationBuilder locationBuilder = createLocationResource(parser, fhirResourceFiler, csvHelper);
        if (locationBuilder == null) {
            return;
        }

        //set the managing organisation for the location, basically itself!
        // If either needs to be mapped then all references need to be local unmapped refs
        boolean mapIds = !(organizationBuilder.isIdMapped() && locationBuilder.isIdMapped());
        // Id possibly remapped to GUID if retrieved from DB
        organizationBuilder = setOrgReferences(organizationBuilder,mapIds,parser,fhirResourceFiler,csvHelper);
        Reference organisationReference;
        if (mapIds) {
            organizationBuilder.setId(parser.getID().getString());
            locationBuilder.setId(parser.getID().getString());
            organisationReference = csvHelper.createOrganisationReference(parser.getID());
        } else {
            organisationReference = csvHelper.createOrganisationReference(organizationBuilder.getResourceId());
        }
//        if (organizationBuilder.isIdMapped()) {
//            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference,fhirResourceFiler);
//        }
        locationBuilder.setManagingOrganisation(organisationReference, parser.getRowIdentifier());
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(),mapIds, organizationBuilder, locationBuilder);

    }

    public static LocationBuilder createLocationResource(SROrganisation parser,
                                                         FhirResourceFiler fhirResourceFiler,
                                                         TppCsvHelper csvHelper) throws Exception {

        CsvCell IdCell = parser.getID();
        CsvCell rowIdCell = parser.getRowIdentifier();
        if ((rowIdCell.isEmpty()) || (!StringUtils.isNumeric(rowIdCell.getString()))) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifier: {} in file : {}", rowIdCell.getString(), parser.getFilePath());
            return null;
        }

        LocationBuilder locationBuilder = LocationResourceCache.getLocationBuilder(IdCell, csvHelper,fhirResourceFiler);
        boolean mapIds = true;
        if (locationBuilder.isIdMapped()) {
            locationBuilder.setId(locationBuilder.getResource().getId());
            mapIds = false;
        }

        CsvCell obsoleteCell = parser.getMadeObsolete();
        if (!obsoleteCell.isEmpty() && obsoleteCell.getBoolean()) {
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), mapIds, locationBuilder);
            return null;
        }

        CsvCell nameCell = parser.getName();
        if (!nameCell.getString().isEmpty()) {
            locationBuilder.setName(nameCell.getString());
        }

        CsvCell locationIdCell = parser.getID();
        if (locationIdCell.isEmpty()) {
            return null;
        }
        locationBuilder.setId(locationIdCell.getString(), locationIdCell);

        if (!locationBuilder.getAddresses().isEmpty()) {
            locationBuilder.removeAddress(null);
        }
        AddressBuilder addressBuilder = new AddressBuilder(locationBuilder);
        addressBuilder.setId(IdCell.getString(), IdCell);
        addressBuilder.setUse(Address.AddressUse.HOME);
        CsvCell nameOfBuildingCell = parser.getHouseName();
        if (!nameOfBuildingCell.isEmpty()) {
            addressBuilder.addLine(nameOfBuildingCell.getString(), nameOfBuildingCell);
        }
        CsvCell numberOfBuildingCell = parser.getHouseNumber();
        CsvCell nameOfRoadCell = parser.getNameOfRoad();
        StringBuilder next = new StringBuilder();
        // Some addresses have a house name with or without a street number or road name
        // Try to handle combinations
        if (!numberOfBuildingCell.isEmpty()) {
            next.append(numberOfBuildingCell.getString());
        }
        if (!nameOfRoadCell.isEmpty()) {
            next.append(" ");
            next.append(nameOfRoadCell.getString());
        }
        if (next.length() > 0) {
            addressBuilder.addLine(next.toString());
        }
        CsvCell nameOfLocalityCell = parser.getNameOfLocality();
        if (!nameOfLocalityCell.isEmpty()) {
            addressBuilder.addLine(nameOfLocalityCell.getString(), nameOfLocalityCell);
        }
        CsvCell nameOfTownCell = parser.getNameOfTown();
        if (!nameOfTownCell.isEmpty()) {
            addressBuilder.addLine(nameOfTownCell.getString(), nameOfTownCell);
        }
        CsvCell nameOfCountyCell = parser.getNameOfCounty();
        if (!nameOfCountyCell.isEmpty()) {
            addressBuilder.addLine(nameOfCountyCell.getString(), nameOfCountyCell);
        }

        CsvCell fullPostCodeCell = parser.getFullPostCode();
        if (!fullPostCodeCell.isEmpty()) {
            addressBuilder.addLine(fullPostCodeCell.getString(), fullPostCodeCell);
        }

        CsvCell contactNumberCell = parser.getTelephone();
        if (!contactNumberCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.PHONE, contactNumberCell, IdCell, locationBuilder);
        }

        CsvCell secondaryContactCell = parser.getSecondaryTelephone();
        if (!secondaryContactCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.PHONE, secondaryContactCell, IdCell, locationBuilder);
        }

        CsvCell faxCell = parser.getFax();
        if (!faxCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.FAX, faxCell, IdCell, locationBuilder);
        }


        return locationBuilder;
    }

    public static OrganizationBuilder createOrganisationResource(SROrganisation parser,
                                                                 FhirResourceFiler fhirResourceFiler,
                                                                 TppCsvHelper csvHelper) throws Exception {

        CsvCell IdCell = parser.getID();
        CsvCell rowIdCell = parser.getRowIdentifier();

        if ((rowIdCell.isEmpty()) || (!StringUtils.isNumeric(rowIdCell.getString()))) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifer: {} in file : {}", rowIdCell.getString(), parser.getFilePath());
            return null;
        }
        CsvCell organizationId = parser.getID();

        if ((organizationId.isEmpty()) || (!StringUtils.isNumeric(organizationId.getString()))) {
           // TransformWarnings.log(LOG, parser, "ERROR: missing or invalid Organization Id: {} in file : {}", rowIdCell.getString(), parser.getFilePath());
            return null;
        }

        OrganizationBuilder organizationBuilder;
        org.hl7.fhir.instance.model.Organization organization
                = (org.hl7.fhir.instance.model.Organization) csvHelper.retrieveResource(IdCell.getString(), ResourceType.Organization);
        if (organization == null) {
            //if the Organization doesn't exist yet, create a new one
            organizationBuilder = new OrganizationBuilder();
            organizationBuilder.setId(IdCell.getString(), IdCell);
        } else {
            organizationBuilder = new OrganizationBuilder(organization);
            organizationBuilder.setId(organization.getId());
        }

        CsvCell obsoleteCell = parser.getMadeObsolete();
        CsvCell deleted = parser.getRemovedData();

        if ((obsoleteCell != null && !obsoleteCell.isEmpty() && obsoleteCell.getBoolean()) ||
                (deleted != null && !deleted.isEmpty() && deleted.getIntAsBoolean())) {
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(),!organizationBuilder.isIdMapped(), organizationBuilder);
            return null;
        }

        CsvCell nameCell = parser.getName();
        if (!nameCell.getString().isEmpty()) {
            organizationBuilder.setName(nameCell.getString());
        }

        AddressBuilder addressBuilder = new AddressBuilder(organizationBuilder);
        addressBuilder.setId(IdCell.getString(), IdCell);
        addressBuilder.setUse(Address.AddressUse.HOME);
        CsvCell nameOfBuildingCell = parser.getHouseName();
        if (!nameOfBuildingCell.isEmpty()) {
            addressBuilder.addLine(nameOfBuildingCell.getString(), nameOfBuildingCell);
        }
        CsvCell numberOfBuildingCell = parser.getHouseNumber();
        CsvCell nameOfRoadCell = parser.getNameOfRoad();
        StringBuilder next = new StringBuilder();
        // Some addresses have a house name with or without a street number or road name
        // Try to handle combinations
        if (!numberOfBuildingCell.isEmpty()) {
            next.append(numberOfBuildingCell.getString());
        }
        if (!nameOfRoadCell.isEmpty()) {
            next.append(" ");
            next.append(nameOfRoadCell.getString());
        }
        if (next.length() > 0) {
            addressBuilder.addLine(next.toString());
        }
        CsvCell nameOfLocalityCell = parser.getNameOfLocality();
        if (!nameOfLocalityCell.isEmpty()) {
            addressBuilder.addLine(nameOfLocalityCell.getString(), nameOfLocalityCell);
        }
        CsvCell nameOfTownCell = parser.getNameOfTown();
        if (!nameOfTownCell.isEmpty()) {
            addressBuilder.addLine(nameOfTownCell.getString(), nameOfTownCell);
        }
        CsvCell nameOfCountyCell = parser.getNameOfCounty();
        if (!nameOfCountyCell.isEmpty()) {
            addressBuilder.addLine(nameOfCountyCell.getString(), nameOfCountyCell);
        }

        CsvCell fullPostCodeCell = parser.getFullPostCode();
        if (!fullPostCodeCell.isEmpty()) {
            addressBuilder.addLine(fullPostCodeCell.getString(), fullPostCodeCell);
        }

        CsvCell contactNumberCell = parser.getTelephone();
        if (!contactNumberCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.PHONE, contactNumberCell, IdCell, organizationBuilder);
        }

        CsvCell secondaryContactCell = parser.getSecondaryTelephone();
        if (!secondaryContactCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.PHONE, secondaryContactCell, IdCell, organizationBuilder);
        }

        CsvCell faxCell = parser.getFax();
        if (!faxCell.isEmpty()) {
            createContactPoint(ContactPoint.ContactPointSystem.FAX, faxCell, IdCell, organizationBuilder);
        }


        return organizationBuilder;
        //  fhirResourceFiler.saveAdminResource(null, organizationBuilder);
    }

    private static OrganizationBuilder setOrgReferences(OrganizationBuilder organizationBuilder, boolean mapIds,
                                                        SROrganisation parser, FhirResourceFiler fhirResourceFiler,
                                                        TppCsvHelper csvHelper) throws Exception {
        CsvCell trustCell = parser.getIDTrust();
        if (!trustCell.isEmpty() && trustCell.getInt() >= 0) {
            //set the trust as a parent organisation for the organisation
            //Reference trustReference = csvHelper.createOrganisationReference(trustCell);
            String trustString =  TRUST_PREFIX_STRING + trustCell.getString();
            Reference trustReference = ReferenceHelper.createReference(ResourceType.Organization,trustString);
            if (!mapIds) {
                trustReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(trustReference,fhirResourceFiler);
            }
            organizationBuilder.setParentOrganisation(trustReference, trustCell);
        }
        CsvCell ccgCell = parser.getIDCcg();
        if (!ccgCell.isEmpty() && ccgCell.getInt() >= 0) {
            //set the trust as a parent organisation for the organisation
            String ccgString = CCG_PREFIX_STRING + ccgCell.getString();
            Reference ccgReference = ReferenceHelper.createReference(ResourceType.Organization,ccgString);
            if (!mapIds) {
                ccgReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(ccgReference,fhirResourceFiler);
            }
            organizationBuilder.setParentOrganisation(ccgReference, ccgCell);
        }

        return organizationBuilder;
    }

    private static void createContactPoint(ContactPoint.ContactPointSystem system, CsvCell contactCell, CsvCell rowIdCell, HasContactPointI parentBuilder) {

        ContactPoint.ContactPointUse use = ContactPoint.ContactPointUse.WORK;

        ContactPointBuilder contactPointBuilder = new ContactPointBuilder(parentBuilder);
        contactPointBuilder.setId(rowIdCell.getString(), contactCell);
        contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE, contactCell);
        contactPointBuilder.setUse(use, contactCell);

        contactPointBuilder.setValue(contactCell.getString(), contactCell);
    }

}
