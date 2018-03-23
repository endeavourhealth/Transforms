package org.endeavourhealth.transform.tpp.schema.Unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRChildHealthGroupActionSchedulingParameters extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRChildHealthGroupActionSchedulingParameters.class); 

  public SRChildHealthGroupActionSchedulingParameters(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IDChildHealthAction",
                      "ActionPriority",
                      "MinAge",
                      "MaxAge",
                      "MinIntervalAnyVac",
                      "MinIntervalThisVacc",
                      "MinIntervalPart1",
                      "TreatmentSpecificTreatmentCentreName",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getIDChildHealthAction() { return super.getCell("IDChildHealthAction");};
 public CsvCell getActionPriority() { return super.getCell("ActionPriority");};
 public CsvCell getMinAge() { return super.getCell("MinAge");};
 public CsvCell getMaxAge() { return super.getCell("MaxAge");};
 public CsvCell getMinIntervalAnyVac() { return super.getCell("MinIntervalAnyVac");};
 public CsvCell getMinIntervalThisVacc() { return super.getCell("MinIntervalThisVacc");};
 public CsvCell getMinIntervalPart1() { return super.getCell("MinIntervalPart1");};
 public CsvCell getTreatmentSpecificTreatmentCentreName() { return super.getCell("TreatmentSpecificTreatmentCentreName");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRChildHealthGroupActionSchedulingParameters Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
