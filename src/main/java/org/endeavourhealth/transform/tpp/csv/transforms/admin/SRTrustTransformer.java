package org.endeavourhealth.transform.tpp.csv.transforms.admin;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.LocationResourceCache;
import org.endeavourhealth.transform.tpp.cache.OrganisationResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.admin.SRTrust;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SRTrustTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRTrustTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRTrust.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRTrust) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createResource(SRTrust parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        //first up, create the organisation resource
        createOrganisationResource(parser, fhirResourceFiler, csvHelper);

        //then the location and link the two
        createLocationResource(parser, fhirResourceFiler, csvHelper);

    }

    public static void createLocationResource(SRTrust parser,
                                              FhirResourceFiler fhirResourceFiler,
                                              TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();

        if ((rowIdCell.isEmpty()) || (!StringUtils.isNumeric(rowIdCell.getString())) ) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifer: {} in file : {}",rowIdCell.getString(), parser.getFilePath());
            return;
        }

        LocationBuilder locationBuilder = LocationResourceCache.getLocationBuilder(rowIdCell, csvHelper,fhirResourceFiler);

        CsvCell obsoleteCell  = parser.getRemovedData();

        if (!obsoleteCell.isEmpty() && obsoleteCell.getBoolean() ) {
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), locationBuilder);
            return;
        }

        CsvCell nameCell = parser.getName();
        if (!nameCell.getString().isEmpty()) {
            locationBuilder.setName(nameCell.getString());
        }

        CsvCell odsCell = parser.getOdsCode();
        if (!odsCell.isEmpty()) {
            List<Identifier> identifiers = IdentifierBuilder.findExistingIdentifiersForSystem(locationBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
            if (identifiers.size() == 0) {
                IdentifierBuilder identifierBuilder = new IdentifierBuilder(locationBuilder);
                identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
                identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
                identifierBuilder.setValue(odsCell.getString(), odsCell);
            }
        }

        AddressBuilder addressBuilder = new AddressBuilder(locationBuilder);
        addressBuilder.setId(rowIdCell.getString(), rowIdCell);
        addressBuilder.setUse(Address.AddressUse.HOME);
        CsvCell nameOfBuildingCell  = parser.getHouseName();
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
        CsvCell nameOfLocalityCell  = parser.getNameOfLocality();
        if (!nameOfLocalityCell.isEmpty()) {
            addressBuilder.addLine(nameOfLocalityCell.getString(), nameOfLocalityCell);
        }
        CsvCell nameOfTownCell  = parser.getNameOfTown();
        if (!nameOfTownCell.isEmpty()) {
            addressBuilder.addLine(nameOfTownCell.getString(), nameOfTownCell);
        }
        CsvCell nameOfCountyCell  = parser.getNameOfCounty();
        if (!nameOfCountyCell.isEmpty()) {
            addressBuilder.addLine(nameOfCountyCell.getString(), nameOfCountyCell);
        }

        CsvCell fullPostCodeCell  = parser.getFullPostCode();
        if (!fullPostCodeCell.isEmpty()) {
            addressBuilder.addLine(fullPostCodeCell.getString(), fullPostCodeCell);
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
        Reference organisationReference = csvHelper.createOrganisationReference(rowIdCell);
        locationBuilder.setManagingOrganisation(organisationReference, rowIdCell);
    }

    public static void createOrganisationResource(SRTrust parser,
                                                  FhirResourceFiler fhirResourceFiler,
                                                  TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();

        if ((rowIdCell.isEmpty()) || (!StringUtils.isNumeric(rowIdCell.getString())) ) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifer: {} in file : {}",rowIdCell.getString(), parser.getFilePath());
            return;
        }

        OrganizationBuilder organizationBuilder = OrganisationResourceCache.getOrganizationBuilder(rowIdCell, csvHelper, fhirResourceFiler);

        CsvCell deleted  = parser.getRemovedData();

        if (!deleted.isEmpty() && deleted.getBoolean()) {
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), organizationBuilder);
            return;
        }

        CsvCell nameCell = parser.getName();
        if (!nameCell.getString().isEmpty()) {
            organizationBuilder.setName(nameCell.getString());
        }

        CsvCell odsCell = parser.getOdsCode();
        if (!odsCell.isEmpty()) {
            List<Identifier> identifiers = IdentifierBuilder.findExistingIdentifiersForSystem(organizationBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
            if (identifiers.size() == 0) {
                IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
                identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
                identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
                identifierBuilder.setValue(odsCell.getString(), odsCell);
            }
        }

        AddressBuilder addressBuilder = new AddressBuilder(organizationBuilder);
        addressBuilder.setId(rowIdCell.getString(), rowIdCell);
        addressBuilder.setUse(Address.AddressUse.HOME);
        CsvCell nameOfBuildingCell  = parser.getHouseName();
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
        CsvCell nameOfLocalityCell  = parser.getNameOfLocality();
        if (!nameOfLocalityCell.isEmpty()) {
            addressBuilder.addLine(nameOfLocalityCell.getString(), nameOfLocalityCell);
        }
        CsvCell nameOfTownCell  = parser.getNameOfTown();
        if (!nameOfTownCell.isEmpty()) {
            addressBuilder.addLine(nameOfTownCell.getString(), nameOfTownCell);
        }
        CsvCell nameOfCountyCell  = parser.getNameOfCounty();
        if (!nameOfCountyCell.isEmpty()) {
            addressBuilder.addLine(nameOfCountyCell.getString(), nameOfCountyCell);
        }

        CsvCell fullPostCodeCell  = parser.getFullPostCode();
        if (!fullPostCodeCell.isEmpty()) {
            addressBuilder.addLine(fullPostCodeCell.getString(), fullPostCodeCell);
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
