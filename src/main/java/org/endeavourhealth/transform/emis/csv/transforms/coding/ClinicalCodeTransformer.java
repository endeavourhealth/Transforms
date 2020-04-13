package org.endeavourhealth.transform.emis.csv.transforms.coding;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisCodeDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCodeType;
import org.endeavourhealth.core.database.dal.reference.SnomedDalI;
import org.endeavourhealth.core.database.dal.reference.models.SnomedLookup;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.Read2Code;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public abstract class ClinicalCodeTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ClinicalCodeTransformer.class);

    private static SnomedDalI snomedDal = DalProvider.factorySnomedDal();
    private static Map<String, Read2Code> read2Cache = new HashMap<>(); //null values are added so need to use regular hashmap
    private static ReentrantLock read2CacheLock = new ReentrantLock();

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        ClinicalCode parser = (ClinicalCode)parsers.get(ClinicalCode.class);
        if (parser != null) {

            //create the extra file of Snomed terms for all the Code Ids we've got
            File extraColsFile = createExtraColsFile(parser, csvHelper);

            //bulk load the file into the DB
            String filePath = parser.getFilePath();
            Date dataDate = fhirResourceFiler.getDataDate();
            EmisCodeDalI dal = DalProvider.factoryEmisCodeDal();
            dal.updateClinicalCodeTable(filePath, extraColsFile.getAbsolutePath(), dataDate);

            //then load up any missing codes we have and see if they've appeared
            Set<Long> missingCodes = csvHelper.retrieveMissingCodes(EmisCodeType.CLINICAL_CODE);
            Set<Long> foundMissingCodes = new HashSet<>();

            while (parser.nextRecord()) {
                try {
                    CsvCell codeIdCell = parser.getCodeId();
                    Long codeId = codeIdCell.getLong();
                    if (missingCodes.contains(codeId)) {
                        foundMissingCodes.add(codeId);
                    }
                } catch (Exception ex) {

                    //because this file contains key reference data, if there's any errors, just throw up
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }

            //log any found missing codes
            csvHelper.addFoundMissingCodes(foundMissingCodes);

            //delete the file we created
            FileHelper.deleteRecursiveIfExists(extraColsFile);
        }
    }

    /**
     * when bulk loading the Clinical Code file into the DB we also have to supply an additional file of
     * Snomed terms for the codes. This function generates that.
     */
    public static File createExtraColsFile(ClinicalCode parser, EmisCsvHelper csvHelper) throws Exception {

        //use the CSV helper thread pool to perform multiple lookups in parallel
        List<Long> codeIds = new ArrayList<>();
        Map<Long, String> hmAdjustedCodes = new ConcurrentHashMap<>();
        Map<Long, Integer> hmIsEmisCodes = new ConcurrentHashMap<>(); //use int rather than boolean so we end up with 1 or 0 not "true" or "false"
        Map<Long, String> hmSnomedTerms = new ConcurrentHashMap<>();

        try {
            while (parser.nextRecord()) {
                CsvCell codeIdCell = parser.getCodeId();
                CsvCell readCodeCell = parser.getReadTermId();
                CsvCell snomedConceptIdCell = parser.getSnomedCTConceptId();

                long codeId = codeIdCell.getLong().longValue();
                String readCode = readCodeCell.getString();
                Long snomedConceptId = snomedConceptIdCell.getLong();

                codeIds.add(new Long(codeId));

                //perform the lookups in the thread pool
                Task t = new Task(codeId, readCode, snomedConceptId, hmAdjustedCodes, hmIsEmisCodes, hmSnomedTerms);
                csvHelper.submitToThreadPool(t);
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }

        //then write out the results to file
        File tempDir = FileHelper.getTempDir();
        File subTempDir = new File(tempDir, UUID.randomUUID().toString());
        if (!subTempDir.exists()) {
            boolean createDir = subTempDir.mkdirs();
            if (!createDir) {
                throw new Exception("Failed to create temp dir " + subTempDir);
            }
        }

        File dstFile = new File(subTempDir, "EmisCodeExtraCols.csv");

        FileOutputStream fos = new FileOutputStream(dstFile);
        OutputStreamWriter osw = new OutputStreamWriter(fos);
        BufferedWriter bufferedWriter = new BufferedWriter(osw);

        //the Emis records use Windows record separators, so we need to match that otherwise
        //the bulk import routine will fail
        CSVFormat format = EmisCsvToFhirTransformer.CSV_FORMAT
                .withHeader("CodeId", "AdjustedCode", "IsEmisCode", "SnomedTerm")
                .withRecordSeparator("\r\n");

        CSVPrinter printer = new CSVPrinter(bufferedWriter, format);

        for (Long codeId: codeIds) {
            String adjustedCode = hmAdjustedCodes.get(codeId);
            Integer isEmisCode = hmIsEmisCodes.get(codeId);
            String snomedTerm = hmSnomedTerms.get(codeId);

            printer.printRecord(codeId, adjustedCode, isEmisCode, snomedTerm);
        }

        printer.close();

        return dstFile;
    }

    /*private static void transform(ClinicalCode parser,
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


    public static String getClinicalCodeSystemForReadCode(String code) throws Exception {
        Read2Code dbCode = lookupRead2CodeUsingCache(code);
        if (dbCode == null) {
            return FhirCodeUri.CODE_SYSTEM_EMIS_CODE;
        } else {
            return FhirCodeUri.CODE_SYSTEM_READ2;
        }
    }
*/

    private static boolean isEmisCode(String code) throws Exception {
        Read2Code dbCode = lookupRead2CodeUsingCache(code);
        return dbCode == null;
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


    private static String padToFive(String code) {
        while (code.length() < 5) {
            code += ".";
        }
        return code;
    }

/*
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
    }*/

    static class Task implements Callable {

        private long codeId;
        private String readCode;
        private Long snomedConceptId;
        private Map<Long, String> hmAdjustedCodes;
        private Map<Long, Integer> hmIsEmisCodes;
        private Map<Long, String> hmSnomedTerms;

        public Task(long codeId, String readCode, Long snomedConceptId, Map<Long, String> hmAdjustedCodes, Map<Long, Integer> hmIsEmisCodes, Map<Long, String> hmSnomedTerms) {
            this.codeId = codeId;
            this.readCode = readCode;
            this.snomedConceptId = snomedConceptId;
            this.hmAdjustedCodes = hmAdjustedCodes;
            this.hmIsEmisCodes = hmIsEmisCodes;
            this.hmSnomedTerms = hmSnomedTerms;
        }

        @Override
        public Object call() throws Exception {

            try {

                SnomedLookup snomedLookup = snomedDal.getSnomedLookup("" + snomedConceptId);
                if (snomedLookup != null) {
                    String snomedTerm = snomedLookup.getTerm();
                    hmSnomedTerms.put(new Long(codeId), snomedTerm);
                }

                //sanitise the code and work out what coding system it should have
                String adjustedCode = removeSynonymAndPadRead2Code(readCode);
                hmAdjustedCodes.put(new Long(codeId), adjustedCode);

                //check if it's an Emis code or valid Read2
                boolean isEmisCode = isEmisCode(adjustedCode);
                if (isEmisCode) {
                    hmIsEmisCodes.put(new Long(codeId), new Integer(1));
                } else {
                    hmIsEmisCodes.put(new Long(codeId), new Integer(0));
                }


            } catch (Throwable t) {
                String msg = "Error processing Clinical Code ID " + codeId;
                throw new TransformException(msg, t);
            }

            return null;
        }
    }
}
