package org.endeavourhealth.transform.tpp.csv.schema.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SREventLink extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SREventLink.class);

    public SREventLink(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
       if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
               || version.equals(TppCsvToFhirTransformer.VERSION_89)) {
          return new String[]{
                  "RowIdentifier",
                  "IDOrganisationVisibleTo",
                  "DateEventRecorded",
                  "DateEvent",
                  "IDProfileEnteredBy",
                  "IDDoneBy",
                  "TextualEventDoneBy",
                  "IDOrganisationDoneAt",
                  "IDEvent",
                  "IDPatient",
                  "IDReferralIn",
                  "IDOrganisation",
                  "IDAppointment",
                  "IDVisit",
                  "IDOrganisationRegisteredAt",
                  "RemovedData"
          };
       } else if (version.equals(TppCsvToFhirTransformer.VERSION_88)) {
          return new String[]{
                  "RowIdentifier",
                  "IDOrganisationVisibleTo",
                  "DateEventRecorded",
                  "DateEvent",
                  "IDProfileEnteredBy",
                  "IDDoneBy",
                  "TextualEventDoneBy",
                  "IDOrganisationDoneAt",
                  "IDEvent",
                  "IDPatient",
                  "IDReferralIn",
                  "IDOrganisation",
                  "IDAppointment",
                  "IDVisit",
                  "IDOrganisationRegisteredAt"
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
                  "IDEvent",
                  "IDPatient",
                  "IDReferralIn",
                  "IDOrganisation",
                  "IDAppointment",
                  "IDVisit",
                  "IDOrganisationRegisteredAt"
          };
       }
    }
    public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
    public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
    public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");}
    public CsvCell getDateEvent() { return super.getCell("DateEvent");}
    public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");}
    public CsvCell getIDDoneBy() { return super.getCell("IDDoneBy");}
    public CsvCell getTextualEventDoneBy() { return super.getCell("TextualEventDoneBy");}
    public CsvCell getIDOrganisationDoneAt() { return super.getCell("IDOrganisationDoneAt");}
    public CsvCell getIDEvent() { return super.getCell("IDEvent");}
    public CsvCell getIDPatient() { return super.getCell("IDPatient");}
    public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");}
    public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
    public CsvCell getIDAppointment() { return super.getCell("IDAppointment");}
    public CsvCell getIDVisit() { return super.getCell("IDVisit");}
    public CsvCell getIDOrganisationRegisteredAt() { return super.getCell("IDOrganisationRegisteredAt");}
    public CsvCell getRemovedData() { return super.getCell("RemovedData");}

    //TODO fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {return "TPP Event Link Entry file ";}

    @Override
    protected boolean isFileAudited() {return true;}
}
