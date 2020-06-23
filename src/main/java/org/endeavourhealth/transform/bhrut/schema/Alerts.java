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
                "EXTERNAL_ID",
                "PAS_ID",
                "ALERT_TYPE_DESCRIPTION",
                "APPLIED_DTTM",
                "START_DTTM",
                "ALERT_DESCRIPTION",
                "ALERT_COMMENTS",
                "RISK_LEVEL",
                "CLOSED_DTTM",
                "CLOSED_NOTE",
                "DataUpdateStatus"
        };

    }

    public CsvCell getID() {
        CsvCell id = super.getCell("EXTERNAL_ID");
        String newId = "BHRUT-" + id.getString();
        CsvCell ret = new CsvCell(id.getPublishedFileId(), id.getRecordNumber(), id.getColIndex(), newId, id.getParentParser());
        return ret;
    }

    public CsvCell getPasId() {
        return super.getCell("PAS_ID");
    }

    public CsvCell getAlertTypeDescription() {
        return super.getCell("ALERT_TYPE_DESCRIPTION");
    }

    public CsvCell getAppliedDttm() {
        return super.getCell("APPLIED_DTTM");
    }

    public CsvCell getStartDttm() {
        return super.getCell("START_DTTM");
    }

    public CsvCell getAlertDescription() {
        return super.getCell("ALERT_DESCRIPTION");
    }

    public CsvCell getAlertComment() {
        return super.getCell("ALERT_COMMENTS");
    }

    public CsvCell getRiskLevel() {
        return super.getCell("RISK_LEVEL");
    }

    public CsvCell getClosedDttm() {
        return super.getCell("CLOSED_DTTM");
    }

    public CsvCell getClosedNote() {
        return super.getCell("CLOSED_NOTE");
    }

    public CsvCell getDataUpdateStatus() {
        return super.getCell("DataUpdateStatus");
    }

    protected String getFileTypeDescription() {
        return "bhrutAlerts Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
