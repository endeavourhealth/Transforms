package org.endeavourhealth.transform.tpp.csv.schema.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRImmunisation extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRImmunisation.class);

    public SRImmunisation(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        //TODO - update transform to check for null cells when using fields not in the older version
        if (version.equals(TppCsvToFhirTransformer.VERSION_87)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "IDVaccination",
                    "IDImmunisationContent",
                    "Dose",
                    "Location",
                    "Method",
                    "DateExpiry",
                    "ImmsReadCode",
                    "VaccPart",
                    "VaccBatchNumber",
                    "VaccAreaCode",
                    "VaccinationStatus",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "IDVaccination",
                    "IDImmunisationContent",
                    "Dose",
                    "Location",
                    "Method",
                    "DateExpiry",
                    "ImmsReadCode",
                    "VaccPart",
                    "VaccBatchNumber",
                    "VaccAreaCode",
                    "VaccinationStatus",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };
        }
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

    public CsvCell getIDVaccination() {
        return super.getCell("IDVaccination");
    }

    public CsvCell getIDImmunisationContent() {
        return super.getCell("IDImmunisationContent");
    }

    public CsvCell getDose() {
        return super.getCell("Dose");
    }

    public CsvCell getLocation() {
        return super.getCell("Location");
    }

    public CsvCell getMethod() {
        return super.getCell("Method");
    }

    public CsvCell getDateExpiry() {
        return super.getCell("DateExpiry");
    }

    public CsvCell getImmsReadCode() {
        return super.getCell("ImmsReadCode");
    }

    public CsvCell getVaccPart() {
        return super.getCell("VaccPart");
    }

    public CsvCell getVaccBatchNumber() {
        return super.getCell("VaccBatchNumber");
    }

    public CsvCell getVaccAreaCode() {
        return super.getCell("VaccAreaCode");
    }

    public CsvCell getVaccinationStatus() {
        return super.getCell("VaccinationStatus");
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

    public CsvCell getIDOrganisationRegisteredAt() {
        return super.getCell("IDOrganisationRegisteredAt");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }

    //TODO fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {
        return "TPP Immunisation Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
