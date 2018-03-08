package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class BulkDiagnosis extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(BulkDiagnosis.class);

    public static final String DATE_FORMAT = "yyyy-mm-dd";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public BulkDiagnosis(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, BartsCsvToFhirTransformer.CSV_FORMAT, DATE_FORMAT, TIME_FORMAT);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "DiagnosisId",
                "Update_DT_TM",
                "ActiveIndicator",
                "PersonId",
                "EncounterId",
                "MRN",
                "FINNbr",
                "Diagnosis",
                "Qualifier",
                "Confirmation",
                "DiagnosisDate",
                "Classification",
                "Clin_Service",
                "Diag_Type",
                "Rank",
                "Diag_Prnsl",
                "Severity_Class",
                "Severity",
                "Certainty",
                "Probability",
                "Org_Name",
                "DiagnosisCode",
                "Vocabulary",
                "Axis",
                "SecondaryDescription",
                "Source_System",
                "Ctrl_Id",
                "Record_Updated_Dt"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner 2.1 Bulk Diagnosis file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getDiagnosisId() {
        return super.getCell("DiagnosisId");
    }

    public CsvCell getUpdateDateTime() {
        return super.getCell("Update_DT_TM");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ActiveIndicator");
    }

    public CsvCell getPersonId() {
        return super.getCell("PersonId");
    }

    public CsvCell getEncounterId() {
        return super.getCell("EncounterId");
    }
    public CsvCell getLocalPatientId() {
        return super.getCell("MRN");
    }
    public CsvCell getFINNbr() {
        return super.getCell("FINNbr");
    }
    public CsvCell getDiagnosis() {
        return super.getCell("Diagnosis");
    }
    public CsvCell getDiagnosisDate() {
        return super.getCell("DiagnosisDate");
    }
    
    public CsvCell getDiagnosisCode() {
        return super.getCell("DiagnosisCode");
    }
    public CsvCell getVocabulary() {
        return super.getCell("Vocabulary");
    }
    public CsvCell getSecondaryDescription() {
        return super.getCell("SecondaryDescription");
    }

    /*public Long getDiagnosisId() throws FileFormatException {
        return Long.parseLong(super.getString("DiagnosisId").trim().split("\\.")[0]);
    }

    public Date getUpdateDateTime() throws TransformException {
        return super.getDate("Update_DT_TM");
    }

    public String getActiveIndicator() throws FileFormatException {
        return super.getString("ActiveIndicator");
    }

    public boolean isActive() throws FileFormatException {
        int val = super.getInt("ActiveIndicator");
        if (val == 1) {
            return true;
        } else {
            return false;
        }
    }

    public Long getPersonId() throws FileFormatException {
        return Long.parseLong(super.getString("PersonId").trim().split("\\.")[0]);
    }

    public Long getEncounterId() throws FileFormatException {
        return Long.parseLong(super.getString("EncounterId").trim().split("\\.")[0]);
    }
    public String getLocalPatientId() throws FileFormatException {
        return super.getString("MRN").trim();
    }
    public String getFINNbr() throws FileFormatException {
        return super.getString("FINNbr").trim();
    }
    public String getDiagnosis() throws FileFormatException {
        return super.getString("Diagnosis").trim();
    }

    public Date getDiagnosisDate() throws TransformException {
        return super.getDate("DiagnosisDate");
    }
    public String getDiagnosisDateAsString() throws TransformException {
        return super.getString("DiagnosisDate");
    }

    public String getDiagnosisCode() throws FileFormatException {
        return super.getString("DiagnosisCode").trim();
    }
    public String getVocabulary() throws FileFormatException {
        return super.getString("Vocabulary").trim();
    }
    public String getSecondaryDescription() throws FileFormatException {
        return super.getString("SecondaryDescription").trim();
    }*/


}