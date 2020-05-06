package org.endeavourhealth.transform.bhrut.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class episodes extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(episodes.class); 

  public episodes(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    BhrutCsvToFhirTransformer.CSV_FORMAT,
                    BhrutCsvToFhirTransformer.DATE_FORMAT,
                    BhrutCsvToFhirTransformer.TIME_FORMAT);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
            return new String[]{
                      "LineStatus"
                    

            };

        }
 public CsvCell getLineStatus() { return super.getCell("LineStatus");}


    @Override
protected String getFileTypeDescription() {return "bhrutepisodes Entry file ";}

    @Override
protected boolean isFileAudited() {return true;}
        }
