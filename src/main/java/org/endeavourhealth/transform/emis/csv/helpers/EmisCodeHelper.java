package org.endeavourhealth.transform.emis.csv.helpers;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.utility.ExpiringCache;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.models.TransformWarning;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisTransformDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCodeType;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.Read2Code;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.ResourceParser;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.HasCodeableConceptI;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.emis.csv.exceptions.EmisCodeNotFoundException;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCodeType;
import org.hl7.fhir.instance.model.Coding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class EmisCodeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(EmisCodeHelper.class);

    public static final String AUDIT_CLINICAL_CODE_SNOMED_CONCEPT_ID = "snomed_concept_id";
    public static final String AUDIT_CLINICAL_CODE_SNOMED_DESCRIPTION_ID = "snomed_description_id";
    public static final String AUDIT_CLINICAL_CODE_READ_CODE = "read_code";
    public static final String AUDIT_CLINICAL_CODE_READ_TERM = "read_term";
    public static final String AUDIT_DRUG_CODE = "drug_code";
    public static final String AUDIT_DRUG_TERM = "drug_term";

    private static EmisTransformDalI mappingRepository = DalProvider.factoryEmisTransformDal();

    //note that these caches are purposefully static since they apply to ALL emis practices (not all caches do)
    private static final long ONE_HOUR = 60L * 60L * 1000L;
    private static Map<Long, EmisCsvCodeMap> clinicalCodes = new ExpiringCache<>(ONE_HOUR);
    private static Map<Long, EmisCsvCodeMap> medication = new ExpiringCache<>(ONE_HOUR);

    public static CodeableConceptBuilder createCodeableConcept(HasCodeableConceptI resourceBuilder, boolean medication, CsvCell codeIdCell, CodeableConceptBuilder.Tag tag, EmisCsvHelper csvHelper) throws Exception {
        if (codeIdCell.isEmpty()) {
            return null;
        }

        EmisCsvCodeMap codeMap = null;
        if (medication) {
            codeMap = findMedication(codeIdCell);
        } else {
            codeMap = findClinicalCode(codeIdCell);
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



    /*public static String getClinicalCodeSystemForReadCode(EmisCsvCodeMap codeMap) {
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
*/
    private static void applyClinicalCodeMap(CodeableConceptBuilder codeableConceptBuilder, EmisCsvCodeMap codeMap, CsvCell... additionalSourceCells) throws Exception {

        //create a coding for the raw Read code, passing in any additional cells to the main code element
        String readCode = codeMap.getAdjustedCode(); //note we use the adjusted code which has been properly padded to five chars
        String readTerm = codeMap.getReadTerm();
        String readCodeSystem = codeMap.getCodeableConceptSystem();

        //just in case
        if (Strings.isNullOrEmpty(readCodeSystem)
                || Strings.isNullOrEmpty(readCode)) {
            throw new Exception("Null code or system from Emis lookup for code " + codeMap.getReadCode());
        }

        codeableConceptBuilder.addCoding(readCodeSystem);
        codeableConceptBuilder.setCodingCode(readCode, createCsvCell(codeMap, AUDIT_CLINICAL_CODE_READ_CODE, readCode, additionalSourceCells));
        codeableConceptBuilder.setCodingDisplay(readTerm, createCsvCell(codeMap, AUDIT_CLINICAL_CODE_READ_TERM, readTerm));

        //create a coding for the Snomed code
        Long snomedConceptId = codeMap.getSnomedConceptId();
        if (snomedConceptId != null) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode("" + snomedConceptId, createCsvCell(codeMap, AUDIT_CLINICAL_CODE_SNOMED_CONCEPT_ID, snomedConceptId));

            //carry over the Snomed Term than we'll have found when processing the clinical code file from the TRUD data
            String snomedTerm = codeMap.getSnomedTerm();
            if (snomedTerm != null) {
                codeableConceptBuilder.setCodingDisplay(snomedTerm);
            }
        }

        //if we have a separate snomed description, set that in a separate coding object
        Long snomedDescriptionId = codeMap.getSnomedDescriptionId();
        if (snomedDescriptionId != null) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_DESCRIPTION_ID);
            codeableConceptBuilder.setCodingCode("" + snomedDescriptionId, createCsvCell(codeMap, AUDIT_CLINICAL_CODE_SNOMED_DESCRIPTION_ID, snomedDescriptionId));
            //we don't have any term to go with this
        }

        //always use the raw read term for the Codeable Concept text
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



    public static EmisCsvCodeMap findClinicalCode(CsvCell codeIdCell) throws Exception {
        EmisCsvCodeMap ret = clinicalCodes.get(codeIdCell.getLong());
        if (ret == null) {
            ret = mappingRepository.getCodeMapping(false, codeIdCell.getLong());
            if (ret == null) {
                LOG.error("Clinical CodeMap value not found " + codeIdCell.getLong() + " for Record Number " + codeIdCell.getRecordNumber());
                throw new EmisCodeNotFoundException(codeIdCell.getLong().longValue(), EmisCodeType.CLINICAL_CODE, "Clinical code not found");
            }
            clinicalCodes.put(codeIdCell.getLong(), ret);
        }
        return ret;
    }


    public static ClinicalCodeType findClinicalCodeType(CsvCell codeIdCell) throws Exception {

        EmisCsvCodeMap ret = findClinicalCode(codeIdCell);
        String typeStr = ret.getCodeType();
        return ClinicalCodeType.fromValue(typeStr);
    }

    public static EmisCsvCodeMap findMedication(CsvCell codeIdCell) throws Exception {

        EmisCsvCodeMap ret = medication.get(codeIdCell.getLong());
        if (ret == null) {
            ret = mappingRepository.getCodeMapping(true, codeIdCell.getLong());
            /**if (ret == null) {
             throw new Exception("Failed to find drug code for codeId " + codeIdCell.getLong());
             }*/
            if (ret == null) {
                LOG.info("Drug code Map value not found " + codeIdCell.getLong() + " for Record Number " + codeIdCell.getRecordNumber());
                throw new EmisCodeNotFoundException(codeIdCell.getLong().longValue(), EmisCodeType.DRUG_CODE, "Drug code not found");
            }
            medication.put(codeIdCell.getLong(), ret);
        }
        return ret;
    }
}
