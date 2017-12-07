package org.endeavourhealth.transform.vision.schema;

import com.google.common.base.Strings;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.io.File;
import java.util.Date;

public class Journal extends AbstractCsvParser {

    public Journal(String version, File f, boolean openParser) throws Exception {
        super(version, f, openParser, VisionCsvToFhirTransformer.CSV_FORMAT.withHeader(
                "PID",
                "ID",
                "DATE",
                "RECORDED_DATE",
                "CODE",
                "SNOMED_CODE",
                "BNF_CODE",
                "HCP",
                "HCP_TYPE",
                "GMS",
                "EPISODE",
                "TEXT",
                "RUBRIC",
                "DRUG_FORM",
                "DRUG_STRENGTH",
                "DRUG_PACKSIZE",
                "DMD_CODE",
                "IMMS_STATUS",
                "IMMS_COMPOUND",
                "IMMS_SOURCE",
                "IMMS_BATCH",
                "IMMS_REASON",
                "IMMS_METHOD",
                "IMMS_SITE",
                "ENTITY",
                "VALUE1_NAME",
                "VALUE1",
                "VALUE1_UNITS",
                "VALUE2_NAME",
                "VALUE2",
                "VALUE2_UNITS",
                "END_DATE",
                "TIME",
                "CONTEXT",
                "CERTAINTY",
                "SEVERITY",
                "LINKS",
                "LINKS_EXT",
                "SERVICE_ID",
                "ACTION",
                "SUBSET",
                "DOCUMENT_ID"),
                VisionCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD,
                VisionCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "PID",
                "ID",
                "DATE",
                "RECORDED_DATE",
                "CODE",
                "SNOMED_CODE",
                "BNF_CODE",
                "HCP",
                "HCP_TYPE",
                "GMS",
                "EPISODE",
                "TEXT",
                "RUBRIC",
                "DRUG_FORM",
                "DRUG_STRENGTH",
                "DRUG_PACKSIZE",
                "DMD_CODE",
                "IMMS_STATUS",
                "IMMS_COMPOUND",
                "IMMS_SOURCE",
                "IMMS_BATCH",
                "IMMS_REASON",
                "IMMS_METHOD",
                "IMMS_SITE",
                "ENTITY",
                "VALUE1_NAME",
                "VALUE1",
                "VALUE1_UNITS",
                "VALUE2_NAME",
                "VALUE2",
                "VALUE2_UNITS",
                "END_DATE",
                "TIME",
                "CONTEXT",
                "CERTAINTY",
                "SEVERITY",
                "LINKS",
                "LINKS_EXT",
                "SERVICE_ID",
                "ACTION",
                "SUBSET",
                "DOCUMENT_ID"
           };
    }

    public String getObservationID() {return super.getString("ID"); }
    public String getPatientID() {
        return super.getString("PID");
    }
    public String getOrganisationID() {
        return super.getString("SERVICE_ID");
    }
    public Date getEffectiveDateTime() throws TransformException {
        return super.getDateTime("DATE","TIME");
    }
    public Date getEnteredDateTime() throws TransformException {
        return super.getDate("RECORDED_DATE");
    }
    public String getReadCode() {
        String readCode = super.getString("CODE");
        if (!Strings.isNullOrEmpty(readCode)) {
            if (readCode.length() > 5)      //trim the Read code to 5 bytes
                readCode = readCode.substring(0,5);
        }
        return readCode;
    }
    public String getSnomedCode() {
        return super.getString("SNOMED_CODE");
    }

    public String getBNFCode() {
        return super.getString("BNF_CODE");
    }
    public String getClinicianUserID() {
        return super.getString("HCP");
    }

    public String getProblemEpisodicity() {
        return super.getString("EPISODE");      //if CODE = Diagnosis or Problem
    }
    public String getDrugPrescriptionType() {
        return super.getString("EPISODE");      //if CODE = Prescribeable item
    }

    public String getDrugDMDCode() { return super.getString("DMD_CODE"); }

    public String getAssociatedText() {
        return super.getString("TEXT");
    }
    public String getRubric() {
        return super.getString("RUBRIC");
    }

    public Double getValue1() {
        return super.getDouble("VALUE1");   //if drug, then amount, else numeric value for investigation, value, result
    }
    public String getValue1NumericUnit() {
        return super.getString("VALUE1_UNITS");
    }

    public Double getValue2() {
        return super.getDouble("VALUE2");   //if drug, then dosage, else second value for BP or special case
    }
    public String getValue2NumericUnit() {
        return super.getString("VALUE2_UNITS");
    }

    public String getImmsStatus() { return super.getString("IMMS_STATUS"); }
    public String getImmsCompound() { return super.getString("IMMS_COMPOUND"); }
    public String getImmsSource() { return super.getString("IMMS_SOURCE"); }
    public String getImmsBatch() { return super.getString("IMMS_BATCH"); }
    public String getImmsReason() { return super.getString("IMMS_REASON"); }
    public String getImmsMethod() { return super.getString("IMMS_METHOD"); }
    public String getImmsSite() { return super.getString("IMMS_SITE"); }

    public Date getEndDate() throws TransformException {
        return super.getDate("END_DATE");   //Episode end date (F or N) or drug end date (A or I)
    }
    public String getAllergyCertainty() {
        return super.getString("CERTAINTY");
    }
    public String getAllergySeverity() { return super.getString("SEVERITY"); }

    public String getLinks() { return super.getString("LINKS"); }
    public String getAction() { return super.getString("ACTION"); }
    public String getSubset() { return super.getString("SUBSET"); }

    public String getObservationEntity() {
        return super.getString("ENTITY");
    }

    public String getDocumentID() {
        return super.getString("DOCUMENT_ID");
    }
}
