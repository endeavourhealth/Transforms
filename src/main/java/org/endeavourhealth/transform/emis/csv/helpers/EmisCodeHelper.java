package org.endeavourhealth.transform.emis.csv.helpers;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.utility.ExpiringCache;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.models.TransformWarning;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisCodeDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisClinicalCode;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCodeType;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisDrugCode;
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

    //note that these caches are purposefully static since they apply to ALL emis practices (not all caches do)
    private static final long ONE_HOUR = 60L * 60L * 1000L;
    private static Map<Long, EmisClinicalCode> clinicalCodes = new ExpiringCache<>(ONE_HOUR);
    private static Map<Long, EmisDrugCode> medication = new ExpiringCache<>(ONE_HOUR);

    public static CodeableConceptBuilder createCodeableConcept(HasCodeableConceptI resourceBuilder, boolean medication, CsvCell codeIdCell, CodeableConceptBuilder.Tag tag, EmisCsvHelper csvHelper) throws Exception {
        if (codeIdCell.isEmpty()) {
            return null;
        }



        if (medication) {
            EmisDrugCode code = findMedication(codeIdCell);

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(resourceBuilder, tag);
            applyMedicationCodeMap(codeableConceptBuilder, code, codeIdCell);
            return codeableConceptBuilder;

        } else {
            EmisClinicalCode code = findClinicalCode(codeIdCell);

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(resourceBuilder, tag);
            applyClinicalCodeMap(codeableConceptBuilder, code, codeIdCell);
            return codeableConceptBuilder;
        }
    }

    private static void applyClinicalCodeMap(CodeableConceptBuilder codeableConceptBuilder, EmisClinicalCode codeMap, CsvCell... additionalSourceCells) throws Exception {

        //create a coding for the raw Read code, passing in any additional cells to the main code element
        String readCode = codeMap.getAdjustedCode(); //note we use the adjusted code which has been properly padded to five chars
        String readTerm = codeMap.getReadTerm();
        boolean isEmisCode = codeMap.isEmisCode();

        //just in case
        if (Strings.isNullOrEmpty(readCode)) {
            throw new Exception("Null code or system from Emis lookup for code " + codeMap.getReadCode());
        }

        String readCodeSystem = null;
        if (isEmisCode) {
            readCodeSystem = FhirCodeUri.CODE_SYSTEM_EMIS_CODE;
        } else {
            readCodeSystem = FhirCodeUri.CODE_SYSTEM_READ2;
        }

        codeableConceptBuilder.addCoding(readCodeSystem);
        codeableConceptBuilder.setCodingCode(readCode, additionalSourceCells);
        codeableConceptBuilder.setCodingDisplay(readTerm, additionalSourceCells);

        //create a coding for the Snomed code
        Long snomedConceptId = codeMap.getSnomedConceptId();
        if (snomedConceptId != null) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode("" + snomedConceptId, additionalSourceCells);

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
            codeableConceptBuilder.setCodingCode("" + snomedDescriptionId, additionalSourceCells);
            //we don't have any term to go with this
        }

        //always use the raw read term for the Codeable Concept text
        codeableConceptBuilder.setText(readTerm, additionalSourceCells);
    }

    private static void applyMedicationCodeMap(CodeableConceptBuilder codeableConceptBuilder, EmisDrugCode codeMap, CsvCell... additionalSourceCells) throws Exception {

        Long dmdId = codeMap.getDmdConceptId();
        String drugName = codeMap.getDmdTerm();

        //if we have a DM+D ID, then build a proper coding in the codeable concept
        if (dmdId != null) {
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode("" + dmdId, additionalSourceCells);
            codeableConceptBuilder.setCodingDisplay(drugName, additionalSourceCells);
        }

        codeableConceptBuilder.setText(drugName, additionalSourceCells);
    }

    /*private static CsvCell[] createCsvCell(EmisCsvCodeMap codeMap, String fieldName, Object value, CsvCell... additionalSourceCells) throws Exception {

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
    }*/

    public static void applyEthnicity(PatientBuilder patientBuilder, EmisClinicalCode codeMap, CsvCell... sourceCells) throws Exception {
        EthnicCategory ethnicCategory = EmisMappingHelper.findEthnicityCode(codeMap);
        //note, the above may return null if it's one of the "unknown" codes, so we simply clear the field on the resource
        patientBuilder.setEthnicity(ethnicCategory, sourceCells);
    }

    public static void applyMaritalStatus(PatientBuilder patientBuilder, EmisClinicalCode codeMap, CsvCell... sourceCells) throws Exception {
        MaritalStatus maritalStatus = EmisMappingHelper.findMaritalStatus(codeMap);
        //note, the above may return null if it's one of the "unknown" codes, so we simply clear the field on the resource
        patientBuilder.setMaritalStatus(maritalStatus, sourceCells);
    }


    /**
     * finds the code for the code ID but if it's an Emis code works up the hierarchy until it finds a Read 2 code
     * Emis support codes having multiple parents as of mid-2020 so this may return multiple parents - if the
     * code is an Emis code, it will return the first non-Emis code parent for each parent branch
     */
    public static List<EmisClinicalCode> findClinicalCodeOrParentRead2Code(CsvCell codeIdCell) throws Exception {
        List<EmisClinicalCode> ret = new ArrayList<>();
        findClinicalCodeOrParentRead2Code(codeIdCell.getLong(), ret);
        return ret;
    }

    private static void findClinicalCodeOrParentRead2Code(Long codeId, List<EmisClinicalCode> parents) throws Exception {

        EmisClinicalCode codeMapping = findClinicalCode(codeId);

        //if it's a true Read2 code, then just add to the list and return out
        if (!codeMapping.isEmisCode()) {
            parents.add(codeMapping);
            return;
        }

        //if it's not a Read2 code, then we need to find the true-Read2 parents
        List<Long> parentCodes = codeMapping.getParentCodes();
        if (parentCodes == null || parentCodes.isEmpty()) {
            return;
        }

        //recurse to find all Read2 codes for all parents
        for (Long parentCode: parentCodes) {
            findClinicalCodeOrParentRead2Code(parentCode, parents);
        }
    }

    public static EmisClinicalCode findClinicalCode(CsvCell codeIdCell) throws Exception {
        Long codeId = codeIdCell.getLong();
        return findClinicalCode(codeId);
    }

    public static EmisClinicalCode findClinicalCode(Long codeId) throws Exception {

        EmisClinicalCode ret = clinicalCodes.get(codeId);
        if (ret == null) {
            EmisCodeDalI dal = DalProvider.factoryEmisCodeDal();
            ret = dal.getClinicalCode(codeId.longValue());
            if (ret == null) {
                LOG.error("Clinical CodeMap value not found " + codeId);
                throw new EmisCodeNotFoundException(codeId.longValue(), EmisCodeType.CLINICAL_CODE, "Clinical code not found");
            }
            clinicalCodes.put(codeId, ret);
        }
        return ret;
    }


    public static ClinicalCodeType findClinicalCodeType(CsvCell codeIdCell) throws Exception {

        EmisClinicalCode ret = findClinicalCode(codeIdCell);
        String typeStr = ret.getCodeType();
        return ClinicalCodeType.fromValue(typeStr);
    }

    public static EmisDrugCode findMedication(CsvCell codeIdCell) throws Exception {

        Long codeId = codeIdCell.getLong();
        EmisDrugCode ret = medication.get(codeId);
        if (ret == null) {
            EmisCodeDalI dal = DalProvider.factoryEmisCodeDal();
            ret = dal.getDrugCode(codeId.longValue());
            if (ret == null) {
                LOG.info("Drug code Map value not found " + codeIdCell.getLong() + " for Record Number " + codeIdCell.getRecordNumber());
                throw new EmisCodeNotFoundException(codeIdCell.getLong().longValue(), EmisCodeType.DRUG_CODE, "Drug code not found");
            }
            medication.put(codeId, ret);
        }
        return ret;
    }
}
