package org.endeavourhealth.transform.tpp.csv.transforms.Patient;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.PatientResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientAddressHistory;
import org.hl7.fhir.instance.model.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRPatientAddressHistoryTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRPatientAddressHistoryTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRPatientAddressHistoryTransformer.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRPatientAddressHistory)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(SRPatientAddressHistory parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();
        if ((rowIdCell.isEmpty()) || (!StringUtils.isNumeric(rowIdCell.getString())) ) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifier: {} in file : {}",rowIdCell.getString(), parser.getFilePath());
            return;
        }
        CsvCell removeDataCell = parser.getRemovedData();
        if (!removeDataCell.getIntAsBoolean()) {
                    return;
        }


        CsvCell IdPatientCell = parser.getIDPatient();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(IdPatientCell,csvHelper,fhirResourceFiler);
        if (!IdPatientCell.isEmpty()) {

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);
            identifierBuilder.setValue(IdPatientCell.getString(), IdPatientCell);
            } else {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString() ,parser.getFilePath());
            return;
        }

        AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
        addressBuilder.setId(rowIdCell.getString(), rowIdCell);
        addressBuilder.setUse(Address.AddressUse.HOME);
        CsvCell nameOfBuildingCell  = parser.getNameOfBuilding();
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
        CsvCell dateEventCell = parser.getDateEvent();
        if (!dateEventCell.isEmpty()) {
            addressBuilder.setStartDate(dateEventCell.getDate(), dateEventCell);
        }
        CsvCell dateToCell = parser.getDateTo();
        if (!dateToCell.isEmpty()) {
            addressBuilder.setEndDate(dateToCell.getDate(), dateToCell);
        }
        //TODO - Using Address.AddressUse but check to see if fhir Address.type is included in data
        CsvCell addressTypeCell = parser.getAddressType();
        if (!addressTypeCell.isEmpty()) {
            SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(addressTypeCell.getString());
            if (snomedCode != null) {
                try {
                   Address.AddressUse addressUse = Address.AddressUse.fromCode(snomedCode.getConceptCode());
                if (addressUse != null) {
                    addressBuilder.setUse(addressUse, addressTypeCell);
                }
                } catch (Exception ex) {
                    TransformWarnings.log(LOG, parser, "Unrecognized address type {} in file {}",
                            addressTypeCell.getString(), parser.getFilePath());
                    }

            }
        }
    }

}
