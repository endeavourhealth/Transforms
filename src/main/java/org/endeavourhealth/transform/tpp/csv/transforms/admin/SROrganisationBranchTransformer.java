package org.endeavourhealth.transform.tpp.csv.transforms.admin;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.LocationResourceCache;
import org.endeavourhealth.transform.tpp.cache.OrganisationResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.admin.SROrganisationBranch;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SROrganisationBranchTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SROrganisationBranchTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SROrganisationBranch.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SROrganisationBranch) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
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

        if ((!obsoleteCell.isEmpty() && obsoleteCell.getBoolean()) ||
                (!deleted.isEmpty() && deleted.getBoolean())) {
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), locationBuilder);
            return;
        }

        CsvCell nameCell = parser.getBranchName();
        if (!nameCell.isEmpty()) {
            locationBuilder.setName(nameCell.getString());
        }

        CsvCell orgIdCell = parser.getIDOrganisation();
        if (!orgIdCell.isEmpty()) {
            if (!OrganisationResourceCache.OrganizationInCache(orgIdCell)) {
                TransformWarnings.log(LOG,parser, "Organisation id {} not found in cache. Row {} in file {}",
                        orgIdCell.getString(),rowIdCell.getString(),parser.getFilePath());
                return;
            }

            Reference organisationReference = csvHelper.createOrganisationReference(orgIdCell);
            locationBuilder.setManagingOrganisation(organisationReference,orgIdCell);
        } else {
            TransformWarnings.log(LOG,parser,"Missing Organization for row Id {} in {}",
                    rowIdCell.getString(), parser.getFilePath());
            return;
        }
        CsvCell locationIdCell = parser.getID();
        if (!locationIdCell.isEmpty()) {
           locationBuilder.setId(locationIdCell.getString());
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
