package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class PROCE extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(PROCE.class);

    public PROCE(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#PROCEDURE_ID",
                "ACTIVE_IND",
                "EXTRACT_DT_TM",
                "ENCNTR_ID",
                "ENCNTR_SLICE_ID",
                "PROCEDURE_SEQ_NBR",
                "NOMENCLATURE_ID",
                "PROCEDURE_DT_TM",
                "PROCEDURE_HCP_PRSNL_ID",
                "PROCEDURE_TYPE_CD",
                "CONCEPT_CKI_IDENT",
                "PROCEDURE_ENTRY_SEQ_NBR"
        };
    }

    public CsvCell getProcedureID() {
        return super.getCell("#PROCEDURE_ID");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getEncounterId() {
        return super.getCell("ENCNTR_ID");
    }

    public CsvCell getEncounterSliceID() {
        return super.getCell("ENCNTR_SLICE_ID");
    }

    public CsvCell getCDSSequence() {
        return super.getCell("PROCEDURE_SEQ_NBR");
    }

    public CsvCell getNomenclatureID() {
        return super.getCell("NOMENCLATURE_ID");
    }

    public CsvCell getProcedureDateTime() {
        return super.getCell("PROCEDURE_DT_TM");
    }

    public CsvCell getPersonnelId() {
        return super.getCell("PROCEDURE_HCP_PRSNL_ID");
    }

    public CsvCell getProcedureTypeCode() {
        return super.getCell("PROCEDURE_TYPE_CD");
    }

    public CsvCell getConceptCodeIdentifier() {
        return super.getCell("CONCEPT_CKI_IDENT");
    }

    public CsvCell geProcedureCodeSequenceEntryOrder() {
        return super.getCell("PROCEDURE_ENTRY_SEQ_NBR");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner person relationship file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}