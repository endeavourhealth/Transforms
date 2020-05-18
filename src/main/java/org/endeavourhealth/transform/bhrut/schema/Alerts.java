package org.endeavourhealth.transform.bhrut.schema;

import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class Alerts extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(Alerts.class); 

  public Alerts(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    BhrutCsvToFhirTransformer.CSV_FORMAT,
                    BhrutCsvToFhirTransformer.DATE_FORMAT,
                    BhrutCsvToFhirTransformer.TIME_FORMAT);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
            return new String[]{
                      "LineStatus",
                       "EXTERNAL_ID",
                       "PAS_ID",
                       "START_DTTM",
                       "END_DTTM",
                       "CLOSED_DTTM",
                       "CLOSED_NOTE",
                       "ALERT_DESCRIPTION",
                       "ALERT_TYPE_DESCRIPTION",
                       "RISK_LEVEL",
                       "ALERT_COMMENTS"
            };

        }
 public CsvCell getLinestatus() { return super.getCell("LineStatus");}
 public CsvCell getId() { return super.getCell( "EXTERNAL_ID");}
 public CsvCell getPasId() { return super.getCell( "PAS_ID");}
 public CsvCell getStartDttm() { return super.getCell( "START_DTTM");}
 public CsvCell getEndDttm() { return super.getCell( "END_DTTM");}
 public CsvCell getClosedDttm() { return super.getCell( "CLOSED_DTTM");}
 public CsvCell getClosedNote() { return super.getCell( "CLOSED_NOTE");}
 public CsvCell getAlertDescription() { return super.getCell( "ALERT_DESCRIPTION");}
 public CsvCell getAlertTypeDescription() { return super.getCell( "ALERT_TYPE_DESCRIPTION");}
 public CsvCell getRiskLevel() { return super.getCell( "RISK_LEVEL");}
 public CsvCell getAlertComments() { return super.getCell( "ALERT_COMMENTS");}



protected String getFileTypeDescription() {return "bhrutAlerts Entry file ";}

    @Override
protected boolean isFileAudited() {return true;}
        }
