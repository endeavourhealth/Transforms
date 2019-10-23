package org.endeavourhealth.transform.emis.csv.helpers;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.core.database.dal.audit.models.TransformWarning;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.ResourceParser;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.HasCodeableConceptI;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.Coding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class EmisCodeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(EmisCodeHelper.class);

    public static final String AUDIT_CLINICAL_CODE_SNOMED_CONCEPT_ID = "snomed_concept_id";
    public static final String AUDIT_CLINICAL_CODE_SNOMED_DESCRIPTION_ID = "snomed_description_id";
    public static final String AUDIT_CLINICAL_CODE_READ_CODE = "read_code";
    public static final String AUDIT_CLINICAL_CODE_READ_TERM = "read_term";
    public static final String AUDIT_DRUG_CODE = "drug_code";
    public static final String AUDIT_DRUG_TERM = "drug_term";


    public static CodeableConceptBuilder createCodeableConcept(HasCodeableConceptI resourceBuilder, boolean medication, CsvCell codeIdCell, CodeableConceptBuilder.Tag tag, EmisCsvHelper csvHelper) throws Exception {
        if (codeIdCell.isEmpty()) {
            return null;
        }

        EmisCsvCodeMap codeMap = null;
        if (medication) {
            codeMap = csvHelper.findMedication(codeIdCell);
        } else {
            codeMap = csvHelper.findClinicalCode(codeIdCell);
        }

        return applyCodeMap(resourceBuilder, codeMap, tag, codeIdCell);
    }

    private static CodeableConceptBuilder applyCodeMap(HasCodeableConceptI resourceBuilder, EmisCsvCodeMap codeMap, CodeableConceptBuilder.Tag tag, CsvCell... additionalSourceCells) throws Exception {

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(resourceBuilder, tag);

        if (codeMap.isMedication()) {
            applyMedicationCodeMap(codeableConceptBuilder, codeMap, additionalSourceCells);

        } else {
            applyClinicalCodeMap(codeableConceptBuilder, codeMap, additionalSourceCells);
        }

        return codeableConceptBuilder;
    }

    public static String removeSynonymAndPadRead2Code(EmisCsvCodeMap codeMap) {
        String code = codeMap.getReadCode();
        return removeSynonymAndPadRead2Code(code);
    }

    public static String removeSynonymAndPadRead2Code(String code) {

        if (Strings.isNullOrEmpty(code)) {
            return code;
        }

        //the CSV uses a hyphen to delimit the synonym ID from the code so detect this and substring accordingly
        int index = code.indexOf("-");
        if (index > -1) {
            code = code.substring(0, index);
        }

        //pad up to five chars so it's in the standard code format (e.g. 4JD -> 4JD..)
        while (code.length() < 5) {
            code += ".";
        }

        return code;
    }



    public static String getClinicalCodeSystemForReadCode(EmisCsvCodeMap codeMap) {
        //without a Read 2 engine, there seems to be no cast-iron way to determine whether the supplied codes
        //are Read 2 codes or Emis local codes. Looking at the codes from the test data sets, this seems
        //to be a reliable way to perform the same check.

        String readCode = removeSynonymAndPadRead2Code(codeMap);

        if (readCode.startsWith("EMIS")
                || readCode.startsWith("ALLERGY")
                || readCode.startsWith("EGTON")
                || readCode.length() > 5) {

            return FhirCodeUri.CODE_SYSTEM_EMIS_CODE;

        } else {
            //if valid Read2
            return FhirCodeUri.CODE_SYSTEM_READ2;
        }
    }

    private static void applyClinicalCodeMap(CodeableConceptBuilder codeableConceptBuilder, EmisCsvCodeMap codeMap, CsvCell... additionalSourceCells) throws Exception {

        String readCode = removeSynonymAndPadRead2Code(codeMap);
        String readTerm = codeMap.getReadTerm();
        Long snomedConceptId = codeMap.getSnomedConceptId();
        String snomedTerm = codeMap.getSnomedTerm();
        Long snomedDescriptionId = codeMap.getSnomedDescriptionId();

        //create a coding for the raw Read code, passing in any additional cells to the main code element
        codeableConceptBuilder.addCoding(getClinicalCodeSystemForReadCode(codeMap));
        codeableConceptBuilder.setCodingCode(readCode, createCsvCell(codeMap, AUDIT_CLINICAL_CODE_READ_CODE, readCode, additionalSourceCells));
        codeableConceptBuilder.setCodingDisplay(readTerm, createCsvCell(codeMap, AUDIT_CLINICAL_CODE_READ_TERM, readTerm));

        //create a coding for the Snomed code
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
        codeableConceptBuilder.setCodingCode("" + snomedConceptId, createCsvCell(codeMap, AUDIT_CLINICAL_CODE_SNOMED_CONCEPT_ID, snomedConceptId));
        if (snomedTerm == null) {
            //the snomed term will be null if we couldn't find one in our own official Snomed dictionary, in which case use the read term
            codeableConceptBuilder.setCodingDisplay(readTerm, createCsvCell(codeMap, AUDIT_CLINICAL_CODE_READ_TERM, readTerm));

        } else {
            //if we have a snomed term, it came from our official Snomed dictionary, not the source CSV, so don't pass in any CSV cell
            codeableConceptBuilder.setCodingDisplay(snomedTerm);
        }

        //if we have a separate snomed description, set that in a separate coding object
        if (snomedDescriptionId != null) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_DESCRIPTION_ID);
            codeableConceptBuilder.setCodingCode("" + snomedDescriptionId, createCsvCell(codeMap, AUDIT_CLINICAL_CODE_SNOMED_DESCRIPTION_ID, snomedDescriptionId));
        }

        codeableConceptBuilder.setText(readTerm, createCsvCell(codeMap, AUDIT_CLINICAL_CODE_READ_TERM, readTerm));
    }

    private static void applyMedicationCodeMap(CodeableConceptBuilder codeableConceptBuilder, EmisCsvCodeMap codeMap, CsvCell... additionalSourceCells) throws Exception {

        Long dmdId = codeMap.getSnomedConceptId();
        String drugName = codeMap.getSnomedTerm();

        //if we have a DM+D ID, then build a proper coding in the codeable concept
        if (dmdId != null) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode("" + dmdId, createCsvCell(codeMap, AUDIT_DRUG_CODE, dmdId, additionalSourceCells));
            codeableConceptBuilder.setCodingDisplay(drugName, createCsvCell(codeMap, AUDIT_DRUG_TERM, drugName));
            codeableConceptBuilder.setText(drugName, createCsvCell(codeMap, AUDIT_DRUG_TERM, drugName));

        } else {
            //if we don't have a DM+D ID, then just pass in the additional source cells (i.e. the codeId cell) along with the drug name
            codeableConceptBuilder.setText(drugName, createCsvCell(codeMap, AUDIT_DRUG_TERM, drugName, additionalSourceCells));
        }
    }

    private static CsvCell[] createCsvCell(EmisCsvCodeMap codeMap, String fieldName, Object value, CsvCell... additionalSourceCells) throws Exception {

        List<CsvCell> list = new ArrayList<>(Arrays.asList(additionalSourceCells));

        ResourceFieldMappingAudit audit = codeMap.getAudit();
        //audit may be null if the coding file was processed before the audit was added
        if (audit != null) {
            for (ResourceFieldMappingAudit.ResourceFieldMappingAuditRow rowAudit: audit.getAudits()) {
                for (ResourceFieldMappingAudit.ResourceFieldMappingAuditCol colAudit : rowAudit.getCols()) {
                    String field = colAudit.getField();
                    if (field.equals(fieldName)) {
                        int colIndex = colAudit.getCol();
                        int publishedFileId = rowAudit.getFileId();
                        if (publishedFileId > 0) {
                            CsvCell cell = new CsvCell(publishedFileId, rowAudit.getRecord(), colIndex, value.toString(), null);
                            list.add(cell);
                        } else if (rowAudit.getOldStyleAuditId() != null) {
                            //temporary, until all audits are converted over to new style
                            CsvCell cell = CsvCell.factoryOldStyleAudit(rowAudit.getOldStyleAuditId(), colIndex, value.toString(), null);
                            list.add(cell);
                        } else {
                            throw new Exception("No published record ID in audit for EmisCsvCodeMap " + codeMap.getCodeId());
                        }
                    }
                }
            }
        }

        return list.toArray(new CsvCell[0]);
    }

    public static void applyEthnicity(PatientBuilder patientBuilder, EmisCsvCodeMap codeMap, CsvCell... sourceCells) throws Exception {
        EthnicCategory ethnicCategory = EmisMappingHelper.findEthnicityCode(codeMap);
        //note, the above may return null if it's one of the "unknown" codes, so we simply clear the field on the resource
        patientBuilder.setEthnicity(ethnicCategory, sourceCells);
    }

    public static void applyMaritalStatus(PatientBuilder patientBuilder, EmisCsvCodeMap codeMap, CsvCell... sourceCells) throws Exception {
        MaritalStatus maritalStatus = EmisMappingHelper.findMaritalStatus(codeMap);
        //note, the above may return null if it's one of the "unknown" codes, so we simply clear the field on the resource
        patientBuilder.setMaritalStatus(maritalStatus, sourceCells);
    }



}
