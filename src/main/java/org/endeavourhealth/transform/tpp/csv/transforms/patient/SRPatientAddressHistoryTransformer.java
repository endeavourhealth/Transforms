package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.PatientResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientAddressHistory;
import org.hl7.fhir.instance.model.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SRPatientAddressHistoryTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRPatientAddressHistoryTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRPatientAddressHistory.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRPatientAddressHistory) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createResource(SRPatientAddressHistory parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();
        if ((rowIdCell.isEmpty()) || (!StringUtils.isNumeric(rowIdCell.getString()))) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifier: {} in file : {}", rowIdCell.getString(), parser.getFilePath());
            return;
        }

        CsvCell IdPatientCell = parser.getIDPatient();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(IdPatientCell, csvHelper, fhirResourceFiler);
        CsvCell removeDataCell = parser.getRemovedData();
        if ((removeDataCell != null) && !removeDataCell.isEmpty() && removeDataCell.getIntAsBoolean()) {
            List<Address> addresses = patientBuilder.getAddresses();
            for (Address address : addresses) {
                if (address.getId().equals(rowIdCell.getString())) {
                    patientBuilder.removeAddress(address);
                }
            }
            return;
        }
        if (IdPatientCell.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
        addressBuilder.setId(rowIdCell.getString(), rowIdCell);

        CsvCell dateToCell = parser.getDateTo();
        CsvCell addressTypeCell = parser.getAddressType();

        Address.AddressUse addressUse;

        if (!addressTypeCell.isEmpty()) {
            addressUse = getAddressUse(addressTypeCell,dateToCell,parser, csvHelper);
            addressBuilder.setUse(addressUse);
                }

        if (!dateToCell.isEmpty()) {
            addressBuilder.setEndDate(dateToCell.getDate(), dateToCell);
        }
        CsvCell nameOfBuildingCell = parser.getNameOfBuilding();
        if (!nameOfBuildingCell.isEmpty()) {
            addressBuilder.addLine(nameOfBuildingCell.getString(), nameOfBuildingCell);
        }
        CsvCell numberOfBuildingCell = parser.getNumberOfBuilding();
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
        CsvCell dateEventCell = parser.getDateEvent();
        if (!dateEventCell.isEmpty()) {
            addressBuilder.setStartDate(dateEventCell.getDate(), dateEventCell);
        }


    }
    private static Address.AddressUse getAddressUse(CsvCell addressTypeCell, CsvCell dateToCell,
                                             SRPatientAddressHistory parser, TppCsvHelper csvHelper)  throws Exception {
        Address.AddressUse addressUse = null;
        try {
            TppMappingRef mapping = csvHelper.lookUpTppMappingRef(addressTypeCell, parser);
            if (mapping != null) {

                addressUse = Address.AddressUse.fromCode(mapping.getMappedTerm().toLowerCase());
                if (addressUse != null) {
                    return addressUse;
                }
            }
        } catch (Exception ex) {
            TransformWarnings.log(LOG, parser, "Unrecognized address type {} in file {}",
                    addressTypeCell.getString(), parser.getFilePath());
        } finally {
            if (addressUse == null) {
                if (dateToCell.isEmpty()) {
                    addressUse = Address.AddressUse.HOME;
                } else {
                    addressUse = Address.AddressUse.OLD;
                }
            }
        }
        return addressUse;
    }
}


