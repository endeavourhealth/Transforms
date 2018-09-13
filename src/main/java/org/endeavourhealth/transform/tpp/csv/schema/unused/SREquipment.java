package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SREquipment extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SREquipment.class);

    public SREquipment(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "RowIdentifier",
                "IDOrganisationVisibleTo",
                "DateEventRecorded",
                "DateEvent",
                "IDProfileEnteredBy",
                "IDDoneBy",
                "TextualEventDoneBy",
                "IDOrganisationDoneAt",
                "EquipmentName",
                "EquipmentID",
                "EquipmentMaintained",
                "IndicativeCost",
                "DirectPayment",
                "EquipmentTrustFunded",
                "IDAssessment",
                "DateStart",
                "DateEnd",
                "EquipmentEndReason",
                "DateApproved",
                "IDProfileApproved",
                "IDEvent",
                "IDPatient",
                "IDOrganisation",
                "RemovedData"


        };

    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getDateEventRecorded() {
        return super.getCell("DateEventRecorded");
    }

    public CsvCell getDateEvent() {
        return super.getCell("DateEvent");
    }

    public CsvCell getIDProfileEnteredBy() {
        return super.getCell("IDProfileEnteredBy");
    }

    public CsvCell getIDDoneBy() {
        return super.getCell("IDDoneBy");
    }

    public CsvCell getTextualEventDoneBy() {
        return super.getCell("TextualEventDoneBy");
    }

    public CsvCell getIDOrganisationDoneAt() {
        return super.getCell("IDOrganisationDoneAt");
    }

    public CsvCell getEquipmentName() {
        return super.getCell("EquipmentName");
    }

    public CsvCell getEquipmentID() {
        return super.getCell("EquipmentID");
    }

    public CsvCell getEquipmentMaintained() {
        return super.getCell("EquipmentMaintained");
    }

    public CsvCell getIndicativeCost() {
        return super.getCell("IndicativeCost");
    }

    public CsvCell getDirectPayment() {
        return super.getCell("DirectPayment");
    }

    public CsvCell getEquipmentTrustFunded() {
        return super.getCell("EquipmentTrustFunded");
    }

    public CsvCell getIDAssessment() {
        return super.getCell("IDAssessment");
    }

    public CsvCell getDateStart() {
        return super.getCell("DateStart");
    }

    public CsvCell getDateEnd() {
        return super.getCell("DateEnd");
    }

    public CsvCell getEquipmentEndReason() {
        return super.getCell("EquipmentEndReason");
    }

    public CsvCell getDateApproved() {
        return super.getCell("DateApproved");
    }

    public CsvCell getIDProfileApproved() {
        return super.getCell("IDProfileApproved");
    }

    public CsvCell getIDEvent() {
        return super.getCell("IDEvent");
    }

    public CsvCell getIDPatient() {
        return super.getCell("IDPatient");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
