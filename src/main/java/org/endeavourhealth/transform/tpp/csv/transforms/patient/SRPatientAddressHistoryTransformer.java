package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientAddressHistory;
import org.hl7.fhir.instance.model.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRPatientAddressHistoryTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRPatientAddressHistoryTransformer.class);

    public static final String ADDRESS_ID_TO_PATIENT_ID = "AddressIdToPatientId";

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

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(SRPatientAddressHistory parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();

        CsvCell removedCell = parser.getRemovedData();
        if (removedCell != null && removedCell.getIntAsBoolean()) {

            //if removed, we won't have the patient ID, so need to look it up
            String patientId = csvHelper.getInternalId(ADDRESS_ID_TO_PATIENT_ID, rowIdCell.getString());
            if (!Strings.isNullOrEmpty(patientId)) {

                CsvCell dummyPatientCell = CsvCell.factoryDummyWrapper(patientId);
                PatientBuilder patientBuilder = csvHelper.getPatientResourceCache().getOrCreatePatientBuilder(dummyPatientCell, csvHelper);
                if (patientBuilder != null) {
                    //remove any existing instance of this address
                    AddressBuilder.removeExistingAddressById(patientBuilder, rowIdCell.getString());
                }
            }
            return;
        }

        CsvCell patientIdCell = parser.getIDPatient();
        PatientBuilder patientBuilder = csvHelper.getPatientResourceCache().getOrCreatePatientBuilder(patientIdCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        //remove any existing instance of this address
        AddressBuilder.removeExistingAddressById(patientBuilder, rowIdCell.getString());

        AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
        addressBuilder.setId(rowIdCell.getString(), rowIdCell);

        CsvCell dateFromCell = parser.getDateEvent();
        if (!dateFromCell.isEmpty()) {
            addressBuilder.setStartDate(dateFromCell.getDate(), dateFromCell);
        }

        CsvCell dateToCell = parser.getDateTo();
        if (!dateToCell.isEmpty()) {
            addressBuilder.setEndDate(dateToCell.getDate(), dateToCell);
        }

        CsvCell addressTypeCell = parser.getAddressType();
        Address.AddressUse addressUse = getAddressUse(addressTypeCell, dateToCell, parser, csvHelper);
        addressBuilder.setUse(addressUse, addressTypeCell);

        CsvCell nameOfBuildingCell = parser.getNameOfBuilding();
        if (!nameOfBuildingCell.isEmpty()) {
            addressBuilder.addLine(nameOfBuildingCell.getString(), nameOfBuildingCell);
        }
        CsvCell numberOfBuildingCell = parser.getNumberOfBuilding();
        CsvCell nameOfRoadCell = parser.getNameOfRoad();
        addressBuilder.addLineFromHouseNumberAndRoad(numberOfBuildingCell, nameOfRoadCell);

        CsvCell nameOfLocalityCell = parser.getNameOfLocality();
        if (!nameOfLocalityCell.isEmpty()) {
            addressBuilder.addLine(nameOfLocalityCell.getString(), nameOfLocalityCell);
        }
        CsvCell nameOfTownCell = parser.getNameOfTown();
        if (!nameOfTownCell.isEmpty()) {
            addressBuilder.setTown(nameOfTownCell.getString(), nameOfTownCell);
        }
        CsvCell nameOfCountyCell = parser.getNameOfCounty();
        if (!nameOfCountyCell.isEmpty()) {
            addressBuilder.setDistrict(nameOfCountyCell.getString(), nameOfCountyCell);
        }
        CsvCell fullPostCodeCell = parser.getFullPostCode();
        if (!fullPostCodeCell.isEmpty()) {
            addressBuilder.setPostcode(fullPostCodeCell.getString(), fullPostCodeCell);
        }

        CsvCell dateEventCell = parser.getDateEvent();
        if (!dateEventCell.isEmpty()) {
            addressBuilder.setStartDate(dateEventCell.getDate(), dateEventCell);
        }
    }

    private static Address.AddressUse getAddressUse(CsvCell addressTypeCell, CsvCell dateToCell,
                                                    SRPatientAddressHistory parser, TppCsvHelper csvHelper) throws Exception {
        Address.AddressUse addressUse = null;
        try {
            TppMappingRef mapping = csvHelper.lookUpTppMappingRef(addressTypeCell);
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


