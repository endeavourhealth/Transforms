package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRQuestionnaire extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRQuestionnaire.class); 

  public SRQuestionnaire(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TppCsvToFhirTransformer.CSV_FORMAT,
                    TppCsvToFhirTransformer.DATE_FORMAT,
                    TppCsvToFhirTransformer.TIME_FORMAT,
                    TppCsvToFhirTransformer.ENCODING);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
            if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_2)) {
                return new String[]{
                        "RowIdentifier",
                        "IDOrganisationVisibleTo",
                        "DtPublished",
                        "IDProfilePublishedBy",
                        "QuestionnaireName",
                        "QuestionnaireVersion",
                        "IDOrganisation"
                };
            } else {
                return new String[]{
                        "RowIdentifier",
                        "IDOrganisationVisibleTo",
                        "DtPublished",
                        "IDProfilePublishedBy",
                        "QuestionnaireName",
                        "QuestionnaireVersion",
                        "IDOrganisation",
                        "RemovedData"
                };
            }

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");}
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");}
 public CsvCell getDtPublished() { return super.getCell("DtPublished");}
 public CsvCell getIDProfilePublishedBy() { return super.getCell("IDProfilePublishedBy");}
 public CsvCell getQuestionnaireName() { return super.getCell("QuestionnaireName");}
 public CsvCell getQuestionnaireVersion() { return super.getCell("QuestionnaireVersion");}
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");}
 public CsvCell getRemovedData() { return super.getCell("RemovedData");}


 //fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRQuestionnaire Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
