package org.endeavourhealth.transform.tpp.csv.transforms.admin;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.LocationResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.admin.SROrganisationBranch;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SROrganisationBranchTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SROrganisationBranchTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SROrganisationBranchTransformer.class);
        while (parser.nextRecord()) {

            try {
                createResource((SROrganisationBranch)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(SROrganisationBranch parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();

        if ((rowIdCell.isEmpty()) || (!StringUtils.isNumeric(rowIdCell.getString())) ) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifer: {} in file : {}",rowIdCell.getString(), parser.getFilePath());
            return;
        }

        LocationBuilder locationBuilder = LocationResourceCache.getLocationBuilder(rowIdCell, csvHelper,fhirResourceFiler);

        CsvCell obsoleteCell  = parser.getBranchObsolete();
        CsvCell deleted = parser.getRemovedData();

        if (obsoleteCell.getBoolean() || deleted.getBoolean() ) {
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), locationBuilder);
            return;
        }

        CsvCell nameCell = parser.getBranchName();
        if (!nameCell.getString().isEmpty()) {
            locationBuilder.setName(nameCell.getString());
        }

        CsvCell orgIdCell = parser.getIDOrganisation();
        if (!orgIdCell.isEmpty()) {
            Reference organisationReference = csvHelper.createOrganisationReference(orgIdCell);
            locationBuilder.setManagingOrganisation(organisationReference,orgIdCell);
        }
        CsvCell locationIdCell = parser.getID();
        if (!locationIdCell.isEmpty()) {
            List<Identifier> identifiers = IdentifierBuilder.findExistingIdentifiersForSystem(locationBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_LOCATION_ID);
            if (identifiers.size() == 0) {
                IdentifierBuilder identifierBuilder = new IdentifierBuilder(locationBuilder);
                identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_LOCATION_ID);
                identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
                identifierBuilder.setValue(locationIdCell.getString(), locationIdCell);
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
        CsvCell nameOfRoadCell = parser.getRoadName();
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
        CsvCell nameOfLocalityCell  = parser.getLocality();
        if (!nameOfLocalityCell.isEmpty()) {
            addressBuilder.addLine(nameOfLocalityCell.getString(), nameOfLocalityCell);
        }
        CsvCell nameOfTownCell  = parser.getTown();
        if (!nameOfTownCell.isEmpty()) {
            addressBuilder.addLine(nameOfTownCell.getString(), nameOfTownCell);
        }
        CsvCell nameOfCountyCell  = parser.getCounty();
        if (!nameOfCountyCell.isEmpty()) {
            addressBuilder.addLine(nameOfCountyCell.getString(), nameOfCountyCell);
        }
        CsvCell fullPostCodeCell  = parser.getPostCode();
        if (!fullPostCodeCell.isEmpty()) {
            addressBuilder.addLine(fullPostCodeCell.getString(), fullPostCodeCell);
        }




    }

}
