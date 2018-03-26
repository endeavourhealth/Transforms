package org.endeavourhealth.transform.tpp.csv.schema.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRDrugSensitivity extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRDrugSensitivity.class); 

  public SRDrugSensitivity(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TppCsvToFhirTransformer.CSV_FORMAT,
                    TppCsvToFhirTransformer.DATE_FORMAT,
                    TppCsvToFhirTransformer.TIME_FORMAT);
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
                      "DateStarted",
                      "DateEnded",
                      "FormulationSpecific",
                      "IDDrugCode",
                      "IDMultiLexAction",
                      "IDReferralIn",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateEventRecorded() { return super.getCell("DateEventRecorded");};
 public CsvCell getDateEvent() { return super.getCell("DateEvent");};
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");};
 public CsvCell getIDDoneBy() { return super.getCell("IDDoneBy");};
 public CsvCell getTextualEventDoneBy() { return super.getCell("TextualEventDoneBy");};
 public CsvCell getIDOrganisationDoneAt() { return super.getCell("IDOrganisationDoneAt");};
 public CsvCell getDateStarted() { return super.getCell("DateStarted");};
 public CsvCell getDateEnded() { return super.getCell("DateEnded");};
 public CsvCell getFormulationSpecific() { return super.getCell("FormulationSpecific");};
 public CsvCell getIDDrugCode() { return super.getCell("IDDrugCode");};
 public CsvCell getIDMultiLexAction() { return super.getCell("IDMultiLexAction");};
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP Drug Sensitivity Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
