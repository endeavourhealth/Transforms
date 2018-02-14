package org.endeavourhealth.transform.emis.csv.helpers;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.HasCodeableConceptI;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class EmisCodeHelper {

    public static final String AUDIT_CLINICAL_CODE_SNOMED_CONCEPT_ID = "snomed_concept_id";
    public static final String AUDIT_CLINICAL_CODE_SNOMED_DESCRIPTION_ID = "snomed_description_id";
    public static final String AUDIT_CLINICAL_CODE_READ_CODE = "read_code";
    public static final String AUDIT_CLINICAL_CODE_READ_TERM = "read_term";
    public static final String AUDIT_DRUG_CODE = "drug_code";
    public static final String AUDIT_DRUG_TERM = "drug_term";

    public static void createCodeableConcept(HasCodeableConceptI resourceBuilder, boolean medication, CsvCell codeIdCell, String tag, EmisCsvHelper csvHelper) throws Exception {
        if (codeIdCell.isEmpty()) {
            return;
        }

        EmisCsvCodeMap codeMap = null;
        if (medication) {
            codeMap = csvHelper.findMedication(codeIdCell);
        } else {
            codeMap = csvHelper.findClinicalCode(codeIdCell);
        }

        applyCodeMap(resourceBuilder, codeMap, tag, codeIdCell);
    }

    public static void applyCodeMap(HasCodeableConceptI resourceBuilder, EmisCsvCodeMap codeMap, String tag, CsvCell... additionalSourceCells) {
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(resourceBuilder, tag);

        if (codeMap.isMedication()) {
            applyMedicationCodeMap(codeableConceptBuilder, codeMap, additionalSourceCells);

        } else {
            applyClinicalCodeMap(codeableConceptBuilder, codeMap, additionalSourceCells);
        }
    }

    public static String getClinicalCodeSystemForReadCode(EmisCsvCodeMap codeMap) {
        //without a Read 2 engine, there seems to be no cast-iron way to determine whether the supplied codes
        //are Read 2 codes or Emis local codes. Looking at the codes from the test data sets, this seems
        //to be a reliable way to perform the same check.

        //the CSV uses a hyphen to delimit the synonym ID from the code, but since we include
        //the original term text anyway, there's no need to carry the synonym ID into the FHIR data
        String readCode = codeMap.getReadCode();
        int index = readCode.indexOf("-");
        if (index > -1) {
            readCode = readCode.substring(0, index);
        }

        if (readCode.startsWith("EMIS")
                || readCode.startsWith("ALLERGY")
                || readCode.startsWith("EGTON")
                || readCode.length() > 5) {

            return FhirUri.CODE_SYSTEM_EMIS_CODE;

        } else {
            //if valid Read2
            return FhirUri.CODE_SYSTEM_READ2;
        }
    }

    private static String getClinicalCodeSystemForSnomedCode(EmisCsvCodeMap codeMap) {
        String snomedConceptIdStr = "" + codeMap.getSnomedConceptId();
        if (snomedConceptIdStr.length() > 9) {
            //this isn't strictly going to be an Emis snomed code, as we would need
            //to look at the namespace digits in the code to know that an cross-reference against
            //the known Emis namespaces (google "snomed ct namespace registry") but if it's a
            //long-form concept ID, then it's not going to be a standard snomed one
            return FhirUri.CODE_SYSTEM_EMISSNOMED;
        } else {
            return FhirUri.CODE_SYSTEM_SNOMED_CT;
        }
    }

    private static void applyClinicalCodeMap(CodeableConceptBuilder codeableConceptBuilder, EmisCsvCodeMap codeMap, CsvCell... additionalSourceCells) {

        String readCode = codeMap.getReadCode();
        String readTerm = codeMap.getReadTerm();
        Long snomedConceptId = codeMap.getSnomedConceptId();
        String snomedTerm = codeMap.getSnomedTerm();
        Long snomedDescriptionId = codeMap.getSnomedDescriptionId();

        //create a coding for the raw Read code, passing in any additional cells to the main code element
        codeableConceptBuilder.addCoding(getClinicalCodeSystemForReadCode(codeMap));
        codeableConceptBuilder.setCodingCode(readCode, createCsvCell(codeMap, AUDIT_CLINICAL_CODE_READ_CODE, readCode, additionalSourceCells));
        codeableConceptBuilder.setCodingDisplay(readTerm, createCsvCell(codeMap, AUDIT_CLINICAL_CODE_READ_TERM, readTerm));

        //create a coding for the Snomed code
        codeableConceptBuilder.addCoding(getClinicalCodeSystemForSnomedCode(codeMap));
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
            codeableConceptBuilder.addCoding(FhirUri.CODE_SYSTEM_SNOMED_DESCRIPTION_ID);
            codeableConceptBuilder.setCodingCode("" + snomedDescriptionId, createCsvCell(codeMap, AUDIT_CLINICAL_CODE_SNOMED_DESCRIPTION_ID, snomedDescriptionId));
        }

        codeableConceptBuilder.setText(readTerm, createCsvCell(codeMap, AUDIT_CLINICAL_CODE_READ_TERM, readTerm, additionalSourceCells));
    }

    private static void applyMedicationCodeMap(CodeableConceptBuilder codeableConceptBuilder, EmisCsvCodeMap codeMap, CsvCell... additionalSourceCells) {

        Long dmdId = codeMap.getSnomedConceptId();
        String drugName = codeMap.getSnomedTerm();

        //if we have a DM+D ID, then build a proper coding in the codeable concept
        if (dmdId != null) {
            codeableConceptBuilder.addCoding(FhirUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode("" + dmdId, createCsvCell(codeMap, AUDIT_DRUG_CODE, dmdId, additionalSourceCells));
            codeableConceptBuilder.setCodingDisplay(drugName, createCsvCell(codeMap, AUDIT_DRUG_TERM, drugName));
        }

        codeableConceptBuilder.setText(drugName, createCsvCell(codeMap, AUDIT_DRUG_TERM, drugName, additionalSourceCells));
    }

    private static CsvCell[] createCsvCell(EmisCsvCodeMap codeMap, String fieldName, Object value, CsvCell... additionalSourceCells) {

        List<CsvCell> list = new ArrayList<>(Arrays.asList(additionalSourceCells));

        ResourceFieldMappingAudit audit = codeMap.getAudit();
        Map<Long, ResourceFieldMappingAudit.ResourceFieldMappingAuditRow> auditMap = audit.getAudits();
        for (Long key: auditMap.keySet()) {
            ResourceFieldMappingAudit.ResourceFieldMappingAuditRow rowAudit = auditMap.get(key);
            for (ResourceFieldMappingAudit.ResourceFieldMappingAuditCol colAudit: rowAudit.getCols()) {
                String field = colAudit.getField();
                if (field.equals(fieldName)) {
                    int colIndex = colAudit.getCol();
                    long rowAuditId = rowAudit.getAuditId();

                    CsvCell cell = new CsvCell(rowAuditId, colIndex, value.toString(), new SimpleDateFormat(EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD), new SimpleDateFormat(EmisCsvToFhirTransformer.TIME_FORMAT));
                    list.add(cell);
                }
            }
        }

        return list.toArray(new CsvCell[0]);
    }
}
