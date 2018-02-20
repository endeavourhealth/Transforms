package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class DIAGN extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(DIAGN.class);

    public DIAGN(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#DIAGNOSIS_ID",
                "ACTIVE_IND",
                "EXTRACT_DT_TM",
                "ENCNTR_ID",
                "ENCNTR_SLICE_ID",
                "DIAGNOSIS_SEQ_NBR",
                "NOMENCLATURE_ID",
                "DIAGNOSIS_DT_TM",
                "DIAG_HCP_PRSNL_ID",
                "DIAGNOSIS_TYPE_CD",
                "CONCEPT_CKI_IDENT",
                "DIAGNOSIS_TXT",
                "DIAGNOSIS_ENTRY_SEQ_NBR",
        };
    }

    public String getDiagnosisID() {
        return super.getString("#DIAGNOSIS_ID");
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
        return super.getString("DIAGNOSIS_SEQ_NBR");
    }

    public String getNomenclatureID() {
        return super.getString("NOMENCLATURE_ID");
    }

    public Date getDiagnosisDateTime() throws TransformException {
        return super.getDate("DIAGNOSIS_DT_TM");
    }

    public String getDiagnosisDateTimeAsString() {
        return super.getString("DIAGNOSIS_DT_TM");
    }

    public String getPersonnel() {
        return super.getString("DIAG_HCP_PRSNL_ID");
    }

    public Long getDiagnosisTypeCode() {
        return super.getLong("DIAGNOSIS_TYPE_CD");
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

    public String getDiagnosicFreeText() {
        return super.getString("DIAGNOSIS_TXT");
    }

    public String getProcedureCodeSequenceEntryOrder() {
        return super.getString("DIAGNOSIS_ENTRY_SEQ_NBR");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner diagnosis file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}