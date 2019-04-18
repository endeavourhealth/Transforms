package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SURCC extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(SURCC.class);

    public SURCC(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#SURGICAL_CASE_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "PERSON_ID",
                "ENCNTR_ID",
                "SCH_APPT_ID",
                "SURGICAL_CASE_NBR_TXT",
                "PRIMARY_SURGEON_PRSNL_ID",
                "SURGICAL_CASE_SPECIALTY_CD",
                "ASA_CLASS_CD",
                "CASE_LEVEL_CD",
                "PRIORITY_CD",
                "ADD_ON_IND",
                "PATIENT_TYPE_CD",
                "CHECK_IN_DT_TM",
                "CHECK_IN_BY_PRSNL_ID",
                "INSTITUTION_CD",
                "DEPT_CD",
                "SURGICAL_AREA_CD",
                "THEATRE_NBR_CD",
                "SCH_SURGICAL_AREA_CD",
                "SCH_THEATRE_NBR_CD",
                "SCH_ANAES_PRSNL_ID",
                "ANAES_PRSNL_ID",
                "ANAES_INDUCTION_DT_TM",
                "ANAES_STOP_DT_TM",
                "SCH_SURGERY_START_DT_TM",
                "SCH_SURGERY_STOP_DT_TM",
                "SURGERY_START_DT_TM",
                "SURGERY_STOP_DT_TM",
                "CANCELLED_DT_TM",
                "CANCELLED_REASON_CD",
                "CANCELLED_BY_PRSNL_ID",
                "ARRIVAL_DT_TM",
                "TRANS_TO_ANAES_ROOM_DT_TM",
                "PATIENT_IN_OR_DT_TM",
                "PATIENT_OUT_OR_DT_TM",
                "SLOT_TYPE_ID"
        };
    }

    public CsvCell getSurgicalCaseId() {return super.getCell("#SURGICAL_CASE_ID"); }
    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }
    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }
    public CsvCell getPersonId() {
        return super.getCell("PERSON_ID");
    }
    public CsvCell getEncounterId() {
        return super.getCell("ENCNTR_ID");
    }
    public CsvCell getCancelledDateTime() {
        return super.getCell("CANCELLED_DT_TM");
    }
    public CsvCell getInstitutionCode() {
        return super.getCell("INSTITUTION_CD");
    }
    public CsvCell getDepartmentCode() {
        return super.getCell("DEPT_CD");
    }
    public CsvCell getSurgicalAreaCode() {
        return super.getCell("SURGICAL_AREA_CD");
    }
    public CsvCell getTheatreNumberCode() {
        return super.getCell("THEATRE_NBR_CD");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}