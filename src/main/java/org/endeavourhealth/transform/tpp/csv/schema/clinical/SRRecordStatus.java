package org.endeavourhealth.transform.tpp.csv.schema.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRRecordStatus extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRRecordStatus.class);

    public SRRecordStatus(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
       if (version.equals(TppCsvToFhirTransformer.VERSION_87)) {
          return new String[]{
                  "RowIdentifier",
                  "IDOrganisationVisibleTo",
                  "DateEvent",
                  "DateEventRecorded",
                  "IDProfileEnteredBy",
                  "IDDoneBy",
                  "TextualEventDoneBy",
                  "IDOrganisationDoneAt",
                  "MedicalRecordStatus",
                  "IDEvent",
                  "IDPatient",
                  "IDOrganisation",
                  "IDOrganisationRegisteredAt",
                  "RemovedData"
          };
       } else {
          return new String[]{
                  "RowIdentifier",
                  "IDOrganisationVisibleTo",
                  "DateEvent",
                  "DateEventRecorded",
                  "IDProfileEnteredBy",
                  "IDDoneBy",
                  "TextualEventDoneBy",
                  "IDOrganisationDoneAt",
                  "MedicalRecordStatus",
                  "IDEvent",
                  "IDPatient",
                  "IDOrganisation",
                  "IDOrganisationRegisteredAt"
          };
       }
    }
    public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
    public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
    public CsvCell getDateEvent() { return super.getCell("DateEvent");}
    public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");}
    public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");}
    public CsvCell getIDDoneBy() { return super.getCell("IDDoneBy");}
    public CsvCell getTextualEventDoneBy() { return super.getCell("TextualEventDoneBy");}
    public CsvCell getIDOrganisationDoneAt() { return super.getCell("IDOrganisationDoneAt");}
    public CsvCell getMedicalRecordStatus() { return super.getCell("MedicalRecordStatus");}
    public CsvCell getIDEvent() { return super.getCell("IDEvent");}
    public CsvCell getIDPatient() { return super.getCell("IDPatient");}
    public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
    public CsvCell getIDOrganisationRegisteredAt() { return super.getCell("IDOrganisationRegisteredAt");}
    public CsvCell getRemovedData() { return super.getCell("RemovedData");}


    @Override
    protected String getFileTypeDescription() {return "TPP Patient Record Status Entry file ";}

    @Override
    protected boolean isFileAudited() {return true;}
}
