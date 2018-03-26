package org.endeavourhealth.transform.tpp.csv.schema.unused.mentalhealth;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRMHConsent extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRMHConsent.class); 

  public SRMHConsent(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "ConsentType",
                      "ConsentSetting",
                      "ConsentDate",
                      "ConsentExpiryDate",
                      "ReviewDate",
                      "SourceTableSecondOpinionRequestedBy",
                      "SecondOpinionRequestedDate",
                      "ReasonsExplainedToPatientDate",
                      "FormType",
                      "SourceTableSecondOpinion",
                      "IDSecondOpinion",
                      "IDSecondOpinionRequestedBy",
                      "ConsentSecondOpinionDate",
                      "DateEvent",
                      "IDProfileEnteredBy",
                      "IDSection",
                      "IDPatient",
                      "IDOrganisationDoneAt",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getConsentType() { return super.getCell("ConsentType");};
 public CsvCell getConsentSetting() { return super.getCell("ConsentSetting");};
 public CsvCell getConsentDate() { return super.getCell("ConsentDate");};
 public CsvCell getConsentExpiryDate() { return super.getCell("ConsentExpiryDate");};
 public CsvCell getReviewDate() { return super.getCell("ReviewDate");};
 public CsvCell getSourceTableSecondOpinionRequestedBy() { return super.getCell("SourceTableSecondOpinionRequestedBy");};
 public CsvCell getSecondOpinionRequestedDate() { return super.getCell("SecondOpinionRequestedDate");};
 public CsvCell getReasonsExplainedToPatientDate() { return super.getCell("ReasonsExplainedToPatientDate");};
 public CsvCell getFormType() { return super.getCell("FormType");};
 public CsvCell getSourceTableSecondOpinion() { return super.getCell("SourceTableSecondOpinion");};
 public CsvCell getIDSecondOpinion() { return super.getCell("IDSecondOpinion");};
 public CsvCell getIDSecondOpinionRequestedBy() { return super.getCell("IDSecondOpinionRequestedBy");};
 public CsvCell getConsentSecondOpinionDate() { return super.getCell("ConsentSecondOpinionDate");};
 public CsvCell getDateEvent() { return super.getCell("DateEvent");};
 public CsvCell getIDProfileEnteredBy() { return super.getCell("IDProfileEnteredBy");};
 public CsvCell getIDSection() { return super.getCell("IDSection");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisationDoneAt() { return super.getCell("IDOrganisationDoneAt");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRMHConsent Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
