package org.endeavourhealth.transform.emis.csv.transforms.coding;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisTransformDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCodeType;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.database.dal.reference.SnomedDalI;
import org.endeavourhealth.core.database.dal.reference.models.SnomedLookup;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.Read2Code;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCode;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

public abstract class ClinicalCodeTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ClinicalCodeTransformer.class);

    private static SnomedDalI snomedDal = DalProvider.factorySnomedDal();
    private static EmisTransformDalI mappingDal = DalProvider.factoryEmisTransformDal();
    private static Map<String, Read2Code> read2Cache = new HashMap<>(); //null values are added so need to use regular hashmap
    private static ReentrantLock read2CacheLock = new ReentrantLock();

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        try {


            Set<Long> missingCodes = csvHelper.retrieveMissingCodes(EmisCodeType.CLINICAL_CODE);
            Set<Long> foundMissingCodes = new HashSet<>();

            List<EmisCsvCodeMap> mappingsToSave = new ArrayList<>();

            ClinicalCode parser = (ClinicalCode)parsers.get(ClinicalCode.class);
            while (parser != null && parser.nextRecord()) {
                try {
                    transform(parser, fhirResourceFiler, csvHelper, mappingsToSave, missingCodes, foundMissingCodes);
                } catch (Exception ex) {

                    //because this file contains key reference data, if there's any errors, just throw up
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }

            //and save any still pending
            if (!mappingsToSave.isEmpty()) {
                csvHelper.submitToThreadPool(new Task(mappingsToSave));
            }

            //log any found missing codes
            csvHelper.addFoundMissingCodes(foundMissingCodes);

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    private static void transform(ClinicalCode parser,
                                  FhirResourceFiler fhirResourceFiler,
                                  EmisCsvHelper csvHelper,
                                  List<EmisCsvCodeMap> mappingsToSave,
                                  Set<Long> missingCodes,
                                  Set<Long> foundMissingCodes) throws Exception {

        CsvCell codeIdCell = parser.getCodeId();
        CsvCell emisTermCell = parser.getTerm();
        CsvCell emisCodeCell = parser.getReadTermId();
        CsvCell snomedConceptIdCell = parser.getSnomedCTConceptId();
        CsvCell snomedDescriptionIdCell = parser.getSnomedCTDescriptionId();
        CsvCell emisCategoryCell = parser.getEmisCodeCategoryDescription();
        CsvCell nationalCodeCell = parser.getNationalCode();
        CsvCell nationalCodeCategoryCell = parser.getNationalCodeCategory();
        CsvCell nationalCodeDescriptionCell = parser.getNationalDescription();

        ClinicalCodeType codeType = ClinicalCodeType.fromValue(emisCategoryCell.getString());

        EmisCsvCodeMap mapping = new EmisCsvCodeMap();
        mapping.setMedication(false);
        mapping.setCodeId(codeIdCell.getLong().longValue());
        mapping.setCodeType(codeType.getValue());
        mapping.setReadTerm(emisTermCell.getString());
        mapping.setReadCode(emisCodeCell.getString());
        mapping.setSnomedConceptId(snomedConceptIdCell.getLong());
        mapping.setSnomedDescriptionId(snomedDescriptionIdCell.getLong());
        mapping.setNationalCode(nationalCodeCell.getString());
        mapping.setNationalCodeCategory(nationalCodeCategoryCell.getString());
        mapping.setNationalCodeDescription(nationalCodeDescriptionCell.getString());
        mapping.setDtLastReceived(csvHelper.getDataDate());

        //the parent code ID was added after 5.3
        if (parser.getVersion().equals(EmisCsvToFhirTransformer.VERSION_5_4)) {
            CsvCell parentCodeId = parser.getParentCodeId();
            mapping.setParentCodeId(parentCodeId.getLong());
        }

        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();
        auditWrapper.auditValue(emisCodeCell.getPublishedFileId(), emisCodeCell.getRecordNumber(), emisCodeCell.getColIndex(), EmisCodeHelper.AUDIT_CLINICAL_CODE_READ_CODE);
        auditWrapper.auditValue(emisTermCell.getPublishedFileId(), emisTermCell.getRecordNumber(), emisTermCell.getColIndex(), EmisCodeHelper.AUDIT_CLINICAL_CODE_READ_TERM);
        auditWrapper.auditValue(snomedConceptIdCell.getPublishedFileId(), snomedConceptIdCell.getRecordNumber(), snomedConceptIdCell.getColIndex(), EmisCodeHelper.AUDIT_CLINICAL_CODE_SNOMED_CONCEPT_ID);
        auditWrapper.auditValue(snomedDescriptionIdCell.getPublishedFileId(), snomedDescriptionIdCell.getRecordNumber(), snomedDescriptionIdCell.getColIndex(), EmisCodeHelper.AUDIT_CLINICAL_CODE_SNOMED_DESCRIPTION_ID);
        mapping.setAudit(auditWrapper);

        mappingsToSave.add(mapping);
        if (mappingsToSave.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
            List<EmisCsvCodeMap> copy = new ArrayList<>(mappingsToSave);
            mappingsToSave.clear();
            csvHelper.submitToThreadPool(new Task(copy));
        }

        //if this code was previously missing, record that it's been found
        Long codeId = codeIdCell.getLong();
        if (missingCodes.contains(codeId)) {
            foundMissingCodes.add(codeId);
        }
    }

    private static String removeSynonymAndPadRead2Code(String code) throws Exception {

        if (Strings.isNullOrEmpty(code)) {
            return code;
        }

        //the CSV uses a hyphen to delimit the synonym ID from the code so detect this and substring accordingly
        int index = code.indexOf("-");
        if (index == -1) {
            return padToFive(code);
        }

        String prefix = code.substring(0, index);
        String suffix = code.substring(index + 1);

        //if the prefix/suffix match the below then just return the raw code with the hyphen
        if (prefix.startsWith("EMIS") //emis codes
                || prefix.startsWith("ESCT") //stroke code set
                || prefix.startsWith("FUNDHC")
                || prefix.startsWith("JHCC")
                || suffix.equals("KC60") //KC60 community code set
                || suffix.equals("SHHAPT") //sexual health code set
                || prefix.startsWith("RAFME")) {
            return code;
        }

        //the remaining code should ALWAYS be a valid Read2 code, so validate this
        String paddedCode = padToFive(prefix);

        Read2Code dbCode = lookupRead2CodeUsingCache(paddedCode);
        if (dbCode == null) {
            throw new TransformException("Failed to parse non-valid Read2 code [" + code + "], expecting " + paddedCode + " to be valid Read2 code");
        }

        return paddedCode;
    }

    private static Read2Code lookupRead2CodeUsingCache(String code) throws Exception {
        //check the cache first - note that we add lookup failures to the cache too,
        //so we check for if it contains the key rather than if the value is non-null
        try {
            read2CacheLock.lock();
            if (read2Cache.containsKey(code)) {
                return read2Cache.get(code);
            }
        } finally {
            read2CacheLock.unlock();
        }

        //hit the DB to find
        Read2Code dbCode = TerminologyService.lookupRead2Code(code);

        //add to the cache, even if it's null
        try {
            read2CacheLock.lock();
            read2Cache.put(code, dbCode);
        } finally {
            read2CacheLock.unlock();
        }

        return dbCode;
    }

    public static String getClinicalCodeSystemForReadCode(String code) throws Exception {
        Read2Code dbCode = lookupRead2CodeUsingCache(code);
        if (dbCode == null) {
            return FhirCodeUri.CODE_SYSTEM_EMIS_CODE;
        } else {
            return FhirCodeUri.CODE_SYSTEM_READ2;
        }
    }


    private static String padToFive(String code) {
        while (code.length() < 5) {
            code += ".";
        }
        return code;
    }




    static class Task implements Callable {

        private List<EmisCsvCodeMap> mappings = null;

        public Task(List<EmisCsvCodeMap> mappings) {
            this.mappings = mappings;
        }

        @Override
        public Object call() throws Exception {

            try {

                //see if we can find an official snomed term for each concept ID
                for (EmisCsvCodeMap mapping: mappings) {

                    SnomedLookup snomedLookup = snomedDal.getSnomedLookup("" + mapping.getSnomedConceptId());
                    if (snomedLookup != null) {
                        String snomedTerm = snomedLookup.getTerm();
                        mapping.setSnomedTerm(snomedTerm);
                    }

                    //sanitise the code and work out what coding system it should have
                    String readCode = mapping.getReadCode();
                    String adjustedCode = removeSynonymAndPadRead2Code(readCode);
                    String codeableConceptSystem = getClinicalCodeSystemForReadCode(adjustedCode);

                    mapping.setAdjustedCode(adjustedCode);
                    mapping.setCodeableConceptSystem(codeableConceptSystem);
                }

                //and save the mapping batch
                mappingDal.saveCodeMappings(mappings);

            } catch (Throwable t) {
                String msg = "Error saving clinical code records for code IDs ";
                for (EmisCsvCodeMap mapping: mappings) {
                    msg += mapping.getCodeId();
                    msg += ", ";
                }

                LOG.error(msg, t);
                throw new TransformException(msg, t);
            }

            return null;
        }
    }
}
