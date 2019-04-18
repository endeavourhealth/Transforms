package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SURCP extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(SURCP.class);

    public SURCP(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#SURGICAL_CASE_PROC_ID",
                "EXTRACT_DT_TM","ACTIVE_IND",
                "SURGICAL_CASE_ID",
                "ORDER_ID",
                "SCH_PROC_CD",
                "SCH_PROC_CNT_NBR",
                "SCH_PRIMARY_PROC_IND",
                "SCH_MODIFIER_TXT",
                "SCH_PRIMARY_SURGEON_PRSNL_ID",
                "SCH_ANAES_TYPE_CD",
                "SCH_CASE_LEVEL_CD",
                "PROC_CD",
                "PRIMARY_PROC_IND",
                "MODIFIER_TXT",
                "PROC_TXT",
                "PRIMARY_SURGEON_PRSNL_ID",
                "SURGEON_SPECIALTY_CD",
                "PROC_START_DT_TM",
                "PROC_STOP_DT_TM",
                "ANAES_TYPE_CD",
                "WOUND_CLASS_CD",
                "CONCURRENT_IND"
        };
    }

    public CsvCell getSurgicalCaseProcedureId() {
        return super.getCell("#SURGICAL_CASE_PROC_ID");
    }
    public CsvCell getSurgicalCaseId() {
        return super.getCell("SURGICAL_CASE_ID");
    }
    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }
    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getProcedureCode() {
        return super.getCell("PROC_CD");
    }
    public CsvCell getProcedureText() {
        return super.getCell("PROC_TXT");
    }
    public CsvCell getModifierText() {
        return super.getCell("MODIFIER_TXT");
    }
    public CsvCell getPrimaryProcedureIndicator() {
        return super.getCell("PRIMARY_PROC_IND");
    }
    public CsvCell getSurgeonPersonnelId() {
        return super.getCell("PRIMARY_SURGEON_PRSNL_ID");
    }
    public CsvCell getStartDateTime() {
        return super.getCell("PROC_START_DT_TM");
    }
    public CsvCell getStopDateTime() {
        return super.getCell("PROC_STOP_DT_TM");
    }
    public CsvCell getWoundClassCode() {
        return super.getCell("WOUND_CLASS_CD");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}