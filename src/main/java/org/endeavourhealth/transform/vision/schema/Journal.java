package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.util.UUID;

public class Journal extends AbstractCsvParser {

    public Journal(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, VisionCsvToFhirTransformer.CSV_FORMAT.withHeader(
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
                VisionCsvToFhirTransformer.DATE_FORMAT,
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

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getObservationID() {
        return super.getCell("ID");
    }

    public CsvCell getPatientID() {
        return super.getCell("PID");
    }

    public CsvCell getOrganisationID() {
        return super.getCell("SERVICE_ID");
    }

    public CsvCell getEffectiveDateTime() throws TransformException {
        return super.getCell("DATE");
    }

    public CsvCell getEnteredDateTime() throws TransformException {
        return super.getCell("RECORDED_DATE");
    }

    public CsvCell getReadCode() {
        return super.getCell("CODE");
    }

    public CsvCell getSnomedCode() {
        return super.getCell("SNOMED_CODE");
    }

    public CsvCell getBNFCode() {
        return super.getCell("BNF_CODE");
    }

    public CsvCell getClinicianUserID() {
        return super.getCell("HCP");
    }

    public CsvCell getProblemEpisodicity() {
        return super.getCell("EPISODE");
    }   //if CODE = Diagnosis or Problem

    public CsvCell getDrugPrescriptionType() {
        return super.getCell("EPISODE");
    } //if CODE = Prescribeable item

    public CsvCell getDrugDMDCode() {
        return super.getCell("DMD_CODE");
    }

    public CsvCell getDrugPrep() {
        return super.getCell("DRUG_PACKSIZE");
    }   //this is ml, tablet, capsule in test data

    public CsvCell getAssociatedText() {
        return super.getCell("TEXT");
    }     //dosage for medication

    public CsvCell getRubric() {
        return super.getCell("RUBRIC");
    }

    public CsvCell getValue1Name() {
        return super.getCell("VALUE1_NAME");
    }

    public CsvCell getValue1AsText() {
        return super.getCell("VALUE1");
    }

    public CsvCell getValue1() {
        return super.getCell("VALUE1");
    }   //if drug, then amount, else numeric value for investigation, value, result

    public CsvCell getValue1NumericUnit() {
        return super.getCell("VALUE1_UNITS");
    }

    public CsvCell getValue2() {
        return super.getCell("VALUE2");
    }   //second value for BP or special case

    public CsvCell getValue2NumericUnit() {
        return super.getCell("VALUE2_UNITS");
    }

    public CsvCell getImmsStatus() {
        return super.getCell("IMMS_STATUS");
    }

    public CsvCell getImmsCompound() {
        return super.getCell("IMMS_COMPOUND");
    }

    public CsvCell getImmsSource() {
        return super.getCell("IMMS_SOURCE");
    }

    public CsvCell getImmsBatch() {
        return super.getCell("IMMS_BATCH");
    }

    public CsvCell getImmsReason() {
        return super.getCell("IMMS_REASON");
    }

    public CsvCell getImmsMethod() {
        return super.getCell("IMMS_METHOD");
    }

    public CsvCell getImmsSite() {
        return super.getCell("IMMS_SITE");
    }

    public CsvCell getEndDate() throws TransformException {
        return super.getCell("END_DATE");
    }   //Episode end date (F or N) or drug end date (A or I)

    public CsvCell getAllergyCertainty() {
        return super.getCell("CERTAINTY");
    }

    public CsvCell getAllergySeverity() {
        return super.getCell("SEVERITY");
    }

    public CsvCell getLinks() {
        return super.getCell("LINKS");
    }

    public CsvCell getAction() {
        return super.getCell("ACTION");
    }

    public CsvCell getSubset() {
        return super.getCell("SUBSET");
    }

    public CsvCell getObservationEntity() {
        return super.getCell("ENTITY");
    }

    public CsvCell getDocumentID() {
        return super.getCell("DOCUMENT_ID");
    }

//    public String getObservationID() {return super.getString("ID"); }
//    public String getPatientID() {
//        return super.getString("PID");
//    }
//    public String getOrganisationID() {
//        return super.getString("SERVICE_ID");
//    }
//    public Date getEffectiveDateTime() throws TransformException {
//        return super.getDate("DATE");
//    }
//    public Date getEnteredDateTime() throws TransformException {
//        return super.getDate("RECORDED_DATE");
//    }
//    public String getReadCode() {
//        String readCode = super.getString("CODE");
//        if (!Strings.isNullOrEmpty(readCode)) {
//            if (readCode.length() > 5)      //trim the Read code to 5 bytes
//                readCode = readCode.substring(0,5);
//        }
//        return readCode;
//    }
//    public String getSnomedCode() {
//        return super.getString("SNOMED_CODE");
//    }
//
//    public String getBNFCode() {
//        return super.getString("BNF_CODE");
//    }
//
//    public String getClinicianUserID() {
//        return cleanUserId(super.getString("HCP"));
//    }
//
//    public String getProblemEpisodicity() {
//        return super.getString("EPISODE");      //if CODE = Diagnosis or Problem
//    }
//    public String getDrugPrescriptionType() {
//        return super.getString("EPISODE");      //if CODE = Prescribeable item
//    }
//
//    public String getDrugDMDCode() { return super.getString("DMD_CODE"); }
//    public String getDrugPrep() { return super.getString("DRUG_PACKSIZE"); }   //this is ml, tablet, capsule in test data
//
//    public String getAssociatedText() {
//        return super.getString("TEXT");
//    }
//    public String getRubric() {
//        return super.getString("RUBRIC");
//    }
//
//    public String getValue1Name() {
//        return super.getString("VALUE1_NAME");
//    }
//
//    public String getValue1AsText() {
//        return super.getString("VALUE1");
//    }
//
//    public Double getValue1() {
//        return super.getDouble("VALUE1");   //if drug, then amount, else numeric value for investigation, value, result
//    }
//    public String getValue1NumericUnit() {
//        return super.getString("VALUE1_UNITS");
//    }
//
//    public Double getValue2() {
//        return super.getDouble("VALUE2");   //second value for BP or special case
//    }
//    public String getValue2NumericUnit() {
//        return super.getString("VALUE2_UNITS");
//    }
//
//
//
//    public String getImmsStatus() { return super.getString("IMMS_STATUS"); }
//    public String getImmsCompound() { return super.getString("IMMS_COMPOUND"); }
//    public String getImmsSource() { return super.getString("IMMS_SOURCE"); }
//    public String getImmsBatch() { return super.getString("IMMS_BATCH"); }
//    public String getImmsReason() { return super.getString("IMMS_REASON"); }
//    public String getImmsMethod() { return super.getString("IMMS_METHOD"); }
//    public String getImmsSite() { return super.getString("IMMS_SITE"); }
//
//    public Date getEndDate() throws TransformException {
//        return super.getDate("END_DATE");   //Episode end date (F or N) or drug end date (A or I)
//    }
//    public String getAllergyCertainty() {
//        return super.getString("CERTAINTY");
//    }
//    public String getAllergySeverity() { return super.getString("SEVERITY"); }
//
//    public String getLinks() { return super.getString("LINKS"); }
//    public String getAction() { return super.getString("ACTION"); }
//    public String getSubset() { return super.getString("SUBSET"); }
//
//    public String getObservationEntity() {
//        return super.getString("ENTITY");
//    }
//
//    public String getDocumentID() {
//        return super.getString("DOCUMENT_ID");
//    }


}
