package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.util.UUID;

public class Journal extends AbstractCsvParser {

    public Journal(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                VisionCsvToFhirTransformer.CSV_FORMAT.withHeader(getHeaders(version)),
                VisionCsvToFhirTransformer.DATE_FORMAT,
                VisionCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return getHeaders(version);
    }

    private static String[] getHeaders(String version) {

        if (version.equals(VisionCsvToFhirTransformer.VERSION_TEST_PACK)) {
            //the test pack is missing a lot of fields and also represents HCP using their code rather than unique ID
            return new String[]{
                    "PID",
                    "ID",
                    "DATE",
                    "RECORDED_DATE",
                    "CODE",
                    "BNF_CODE",
                    "HCP_CODE", //e.g. Gxxxxxx
                    "HCP_TYPE",
                    "GMS",
                    "EPISODE",
                    "TEXT",
                    "RUBRIC",
                    "VALUE1",
                    "VALUE2",
                    "END_DATE",
                    "TIME",
                    "CONTEXT",
                    "CERTAINTY",
                    "SEVERITY",
                    "LINKS",
                    "SERVICE_ID",
                    "SUBSET",
                    "ACTION"
            };

        } else {
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

    public CsvCell getEffectiveDate() throws TransformException {
        return super.getCell("DATE");
    }

    public CsvCell getEnteredDate() throws TransformException {
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

    /**
     * used in a tiny handful (two) of records out of millions and contains a number
     */
    public CsvCell getLinksExt() {
        return super.getCell("LINKS_EXT");
    }


    public CsvCell getAction() {
        return super.getCell("ACTION");
    }

    public CsvCell getSubset() {
        return super.getCell("SUBSET");
    }

    /**
     * additional classification of the record (e.g. WEIGHT, HEIGHT, PULSE)
     */
    public CsvCell getObservationEntity() {
        return super.getCell("ENTITY");
    }

    public CsvCell getDocumentID() {
        return super.getCell("DOCUMENT_ID");
    }

    public CsvCell getEffectiveTime() {
        return super.getCell("TIME");
    }

    public CsvCell getContext() {
        return super.getCell("CONTEXT");
    }

}
