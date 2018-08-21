package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientAddressHistory;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Reference;
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
            addressBuilder.setStartDate(dateFromCell.getDateTime(), dateFromCell);
        }

        CsvCell dateToCell = parser.getDateTo();
        if (!dateToCell.isEmpty()) {
            addressBuilder.setEndDate(dateToCell.getDate(), dateToCell);
        }

        CsvCell addressTypeCell = parser.getAddressType();
        if (!addressTypeCell.isEmpty()) {
            Address.AddressUse addressUse = null;
            TppMappingRef mapping = csvHelper.lookUpTppMappingRef(addressTypeCell);
            if (mapping != null) {
                String term = mapping.getMappedTerm();
                if (term.equalsIgnoreCase("Home")) {
                    addressUse = Address.AddressUse.HOME;
                } else if (term.equalsIgnoreCase("Temporary")) {
                    addressUse = Address.AddressUse.TEMP;
                } else if (term.equalsIgnoreCase("Official")) {
                    addressUse = Address.AddressUse.TEMP;
                } else if (term.equalsIgnoreCase("Correspondence only")) {
                    addressUse = Address.AddressUse.TEMP;
                } else {
                    TransformWarnings.log(LOG, parser, "Unable to convert address type {} to AddressUse", term);
                }
            }

            //fall back to this in case the above fails
            if (addressUse == null) {
                addressUse = Address.AddressUse.HOME;
            }

            addressBuilder.setUse(addressUse, addressTypeCell);
        }

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
            addressBuilder.setStartDate(dateEventCell.getDateTime(), dateEventCell);
        }

        //note, the managing organisation is set from the SRPatientRegistrationTransformer too, except
        //this means that if a patient doesn't have a record in that file, the mananging org won't get set.
        //So set it here too, on the assumption that a patient will always have an address.
        CsvCell orgIdCell = parser.getIDOrganisation();
        if (!orgIdCell.isEmpty()) {
            Reference orgReferencePatient = csvHelper.createOrganisationReference(orgIdCell);
            if (patientBuilder.isIdMapped()) {
                orgReferencePatient = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReferencePatient, csvHelper);
            }
            patientBuilder.setManagingOrganisation(orgReferencePatient, orgIdCell);
        }

    }

}


