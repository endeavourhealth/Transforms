package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class PROCE extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(PROCE.class);

    public PROCE(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser,
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

    public String getProcedureID() {
        return super.getString("#PROCEDURE_ID");
    }

    public int getActiveIndicator() {
        return super.getInt("ACTIVE_IND");
    }

    public Date getExtractDateTime() throws TransformException {
        return super.getDate("EXTRACT_DT_TM");
    }

    public boolean isActive() {
        int val = super.getInt("ACTIVE_IND");
        if (val == 1) {
            return true;
        } else {
            return false;
        }
    }

    public String getEncounterID() {
        return super.getString("ENCNTR_ID");
    }

    public String getEncounterSliceID() {
        return super.getString("ENCNTR_SLICE_ID");
    }

    public String getCDSSequence() {
        return super.getString("PROCEDURE_SEQ_NBR");
    }

    public String getNomenclatureID() {
        return super.getString("NOMENCLATURE_ID");
    }

    public Date getProcedureDateTime() throws TransformException {
        return super.getDate("");
    }

    public String getProcedureDateTimeAsString() {
        return super.getString("PROCEDURE_DT_TM");
    }

    public String getPersonnelID() {
        return super.getString("PROCEDURE_HCP_PRSNL_ID");
    }

    public Long getProcedureTypeCode() {
        return super.getLong("PROCEDURE_TYPE_CD");
    }

    public String getConceptCodeIdentifier() {
        return super.getString("CONCEPT_CKI_IDENT");
    }

    public String getConceptCodeType() {
        String conceptCodeIdentifier = super.getString("CONCEPT_CKI_IDENT");
        int index = conceptCodeIdentifier.indexOf('!');
        if (index > -1) {
            return conceptCodeIdentifier.substring(0,index);
        } else {
            return null;
        }
    }

    public String getConceptCode() {
        String conceptCodeIdentifier = super.getString("CONCEPT_CKI_IDENT");
        int index = conceptCodeIdentifier.indexOf('!');
        if (index > -1) {
            return conceptCodeIdentifier.substring(index + 1);
        } else {
            return null;
        }
    }

    public String geProcedureCodeSequenceEntryOrder() {
        return super.getString("PROCEDURE_ENTRY_SEQ_NBR");
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