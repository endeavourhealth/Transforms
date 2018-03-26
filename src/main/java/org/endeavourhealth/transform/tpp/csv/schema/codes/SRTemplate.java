package org.endeavourhealth.transform.tpp.csv.schema.codes;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRTemplate extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRTemplate.class); 

  public SRTemplate(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DatePublished",
                      "IDProfilePublishedBy",
                      "TemplateName",
                      "TemplateVersion",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDatePublished() { return super.getCell("DatePublished");};
 public CsvCell getIDProfilePublishedBy() { return super.getCell("IDProfilePublishedBy");};
 public CsvCell getTemplateName() { return super.getCell("TemplateName");};
 public CsvCell getTemplateVersion() { return super.getCell("TemplateVersion");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP Templates Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
