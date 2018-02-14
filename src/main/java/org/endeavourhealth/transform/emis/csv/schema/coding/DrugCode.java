package org.endeavourhealth.transform.emis.csv.schema.coding;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class DrugCode extends AbstractCsvParser {

    public DrugCode(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "CodeId",
                "Term",
                "DmdProductCodeId",
                "ProcessingId"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Emis drug code reference file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getCodeId() {
        return super.getCell("CodeId");
    }
    public CsvCell getTerm() {
        return super.getCell("Term");
    }
    public CsvCell getDmdProductCodeId() {
        return super.getCell("DmdProductCodeId");
    }
    public CsvCell getProcessigId() {
        return super.getCell("ProcessingId");
    }

    /*public Long getCodeId() {
        return super.getLong("CodeId");
    }
    public String getTerm() {
        return super.getString("Term");
    }
    public Long getDmdProductCodeId() {
        return super.getLong("DmdProductCodeId");
    }
    public Integer getProcessigId() {
        return super.getInt("ProcessingId");
    }*/
}
