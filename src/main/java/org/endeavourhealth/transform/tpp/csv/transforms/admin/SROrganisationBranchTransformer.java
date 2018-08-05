package org.endeavourhealth.transform.tpp.csv.transforms.admin;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
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

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(SROrganisationBranch parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();
        CsvCell locationIdCell = parser.getID();

        LocationBuilder locationBuilder = new LocationBuilder();

        //until we get data for a practice with branches, we don't know whether the IDBranch on SREvent
        //refers to the RowID or the ID columns on this table, so I'm setting this up to fail
        //if we get data, so we can look and make the right call
        //It should be:
        //locationBuilder.setId(rowIdCell.getString(), rowIdCell);
        //Or:
        //locationBuilder.setId(locationIdCell.getString(), locationIdCell);
        if (true) {
            throw new TransformException("Don't know what ID to use for Location resource from SROrganisationBranch");
        }

        CsvCell obsoleteCell = parser.getBranchObsolete();
        CsvCell deleted = parser.getRemovedData();

        if ((!obsoleteCell.isEmpty() && obsoleteCell.getBoolean()) ||
                (deleted != null && !deleted.isEmpty() && deleted.getBoolean())) {
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), locationBuilder);
            return;
        }

        CsvCell nameCell = parser.getBranchName();
        if (!nameCell.isEmpty()) {
            locationBuilder.setName(nameCell.getString(), nameCell);
        }

        CsvCell orgIdCell = parser.getIDOrganisation();
        Reference organisationReference = csvHelper.createOrganisationReference(orgIdCell);
        locationBuilder.setManagingOrganisation(organisationReference, orgIdCell);

        AddressBuilder addressBuilder = new AddressBuilder(locationBuilder);
        addressBuilder.setUse(Address.AddressUse.WORK);
        CsvCell nameOfBuildingCell = parser.getHouseName();
        if (!nameOfBuildingCell.isEmpty()) {
            addressBuilder.addLine(nameOfBuildingCell.getString(), nameOfBuildingCell);
        }
        CsvCell numberOfBuildingCell = parser.getHouseNumber();
        CsvCell nameOfRoadCell = parser.getRoadName();
        addressBuilder.addLineFromHouseNumberAndRoad(numberOfBuildingCell, nameOfRoadCell);

        CsvCell nameOfLocalityCell = parser.getLocality();
        if (!nameOfLocalityCell.isEmpty()) {
            addressBuilder.addLine(nameOfLocalityCell.getString(), nameOfLocalityCell);
        }
        CsvCell nameOfTownCell = parser.getTown();
        if (!nameOfTownCell.isEmpty()) {
            addressBuilder.setTown(nameOfTownCell.getString(), nameOfTownCell);
        }
        CsvCell nameOfCountyCell = parser.getCounty();
        if (!nameOfCountyCell.isEmpty()) {
            addressBuilder.setDistrict(nameOfCountyCell.getString(), nameOfCountyCell);
        }
        CsvCell fullPostCodeCell = parser.getPostCode();
        if (!fullPostCodeCell.isEmpty()) {
            addressBuilder.setPostcode(fullPostCodeCell.getString(), fullPostCodeCell);
        }

    }
}
