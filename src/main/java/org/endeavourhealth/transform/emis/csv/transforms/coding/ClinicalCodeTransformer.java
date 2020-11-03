package org.endeavourhealth.transform.emis.csv.transforms.coding;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisCodeDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCodeType;
import org.endeavourhealth.core.database.dal.reference.Read2ToSnomedMapDalI;
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

            Task nextTask = new Task(hmAdjustedCodes, hmIsEmisCodes, hmSnomedTerms);

            while (parser.nextRecord()) {
                CsvCell codeIdCell = parser.getCodeId();
                CsvCell readCodeCell = parser.getReadTermId();
                CsvCell snomedConceptIdCell = parser.getSnomedCTConceptId();

                long codeId = codeIdCell.getLong().longValue();
                String readCode = readCodeCell.getString();
                Long snomedConceptId = snomedConceptIdCell.getLong();

                codeIds.add(new Long(codeId));

                //perform the lookups in the thread pool
                nextTask.addRecord(codeId, readCode, snomedConceptId);
                if (nextTask.isFull()) {
                    csvHelper.submitToThreadPool(nextTask);
                    nextTask = new Task(hmAdjustedCodes, hmIsEmisCodes, hmSnomedTerms);
                }
            }

            //finish off the current task
            if (!nextTask.isEmpty()) {
                csvHelper.submitToThreadPool(nextTask);
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

        String paddedCode = padToFive(prefix);

        //the remaining code should ALWAYS be a valid Read2 code, so validate this
        //note, out of the 1.1M Emis clinical codes, this will only happen for about 18k of them,
        //so it's not too bad that we're looking up these Read2 codes one at a time
        Read2Code dbCode = lookupRead2CodeUsingCache(paddedCode);
        if (dbCode == null) {
            throw new TransformException("Failed to parse non-valid Read2 code [" + code + "], expecting " + paddedCode + " to be valid Read2 code");
        }

        return paddedCode;
    }

    private static Read2Code lookupRead2CodeUsingCache(String code) throws Exception {
        Set<String> s = new HashSet<>();
        s.add(code);
        Map<String, Read2Code> map = lookupRead2CodesUsingCache(s);
        return map.get(code);
    }

    private static Map<String, Read2Code> lookupRead2CodesUsingCache(Collection<String> codes) throws Exception {

        Map<String, Read2Code> ret = new HashMap<>();
        Set<String> codesForDb = new HashSet<>();

        //check the cache first - note that we add lookup failures to the cache too,
        //so we check for if it contains the key rather than if the value is non-null
        try {
            read2CacheLock.lock();

            for (String code: codes) {
                if (read2Cache.containsKey(code)) {
                    Read2Code r2 = read2Cache.get(code);
                    ret.put(code, r2);
                } else {
                    codesForDb.add(code);
                }
            }

        } finally {
            read2CacheLock.unlock();
        }

        //if we have any we need to check the DB for
        if (!codesForDb.isEmpty()) {

            //hit the DB to find any we need
            Read2ToSnomedMapDalI dal = DalProvider.factoryRead2ToSnomedMapDal();
            Map<String, Read2Code> dbMap = dal.getRead2Codes(codesForDb);

            //add to ret
            for (String code: codesForDb) {
                Read2Code r2 = dbMap.get(code);
                ret.put(code, r2);
            }

            //add to the cache, even if it's null
            try {
                read2CacheLock.lock();

                for (String code: codesForDb) {
                    Read2Code r2 = dbMap.get(code);
                    read2Cache.put(code, r2);
                }
            } finally {
                read2CacheLock.unlock();
            }
        }

        return ret;
    }


    private static String padToFive(String code) {
        while (code.length() < 5) {
            code += ".";
        }
        return code;
    }

    static class CodeRecord {
        private long codeId;
        private String readCode;
        private Long snomedConceptId;

        public CodeRecord(long codeId, String readCode, Long snomedConceptId) {
            this.codeId = codeId;
            this.readCode = readCode;
            this.snomedConceptId = snomedConceptId;
        }

        public long getCodeId() {
            return codeId;
        }

        public String getReadCode() {
            return readCode;
        }

        public Long getSnomedConceptId() {
            return snomedConceptId;
        }

        @Override
        public String toString() {
            return "" + codeId;
        }
    }

    static class Task implements Callable {

        private List<CodeRecord> records = new ArrayList<>(); //the codes to look up
        private Map<Long, String> hmAdjustedCodes; //used for results
        private Map<Long, Integer> hmIsEmisCodes; //used for results
        private Map<Long, String> hmSnomedTerms; //used for results

        public Task(Map<Long, String> hmAdjustedCodes, Map<Long, Integer> hmIsEmisCodes, Map<Long, String> hmSnomedTerms) {
            this.hmAdjustedCodes = hmAdjustedCodes;
            this.hmIsEmisCodes = hmIsEmisCodes;
            this.hmSnomedTerms = hmSnomedTerms;
        }

        public void addRecord(long codeId, String readCode, Long snomedConceptId) {
            this.records.add(new CodeRecord(codeId, readCode, snomedConceptId));
        }

        public boolean isFull() {
            return this.records.size() >= TransformConfig.instance().getResourceSaveBatchSize();
        }

        public boolean isEmpty() {
            return this.records.isEmpty();
        }

        @Override
        public Object call() throws Exception {

            try {

                //look up multiple snomed terms at once, as this has shown to be a bottleneck when doing it one concept at a time
                Set<String> conceptIds = new HashSet<>();
                for (CodeRecord record: records) {
                    String conceptId = "" + record.getSnomedConceptId();
                    conceptIds.add(conceptId);
                }
                Map<String, SnomedLookup> snomedMap = snomedDal.getSnomedLookups(conceptIds);
                for (CodeRecord record: records) {
                    long codeId = record.getCodeId();
                    String conceptId = "" + record.getSnomedConceptId();
                    SnomedLookup snomedLookup = snomedMap.get(conceptId);
                    if (snomedLookup != null) {
                        String snomedTerm = snomedLookup.getTerm();
                        hmSnomedTerms.put(new Long(codeId), snomedTerm);
                    }
                }

                //re-format each code so it's in proper Read2 format (e.g. A2 -> A2...)
                for (CodeRecord record: records) {
                    String readCode = record.getReadCode();
                    long codeId = record.getCodeId();
                    String adjustedCode = removeSynonymAndPadRead2Code(readCode);
                    hmAdjustedCodes.put(new Long(codeId), adjustedCode);
                }

                //work out definitely which codes are Emis codes and which are proper Read2
                Set<String> codes = new HashSet<>();
                for (CodeRecord record: records) {
                    long codeId = record.getCodeId();
                    String adjustedCode = hmAdjustedCodes.get(new Long(codeId));
                    codes.add(adjustedCode);
                }
                Map<String, Read2Code> read2Map = lookupRead2CodesUsingCache(codes);
                for (CodeRecord record: records) {
                    long codeId = record.getCodeId();
                    String adjustedCode = hmAdjustedCodes.get(new Long(codeId));
                    Read2Code read2Code = read2Map.get(adjustedCode);
                    boolean isEmisCode = read2Code == null;
                    if (isEmisCode) {
                        hmIsEmisCodes.put(new Long(codeId), new Integer(1));
                    } else {
                        hmIsEmisCodes.put(new Long(codeId), new Integer(0));
                    }
                }

            } catch (Throwable t) {
                String msg = "Error processing Clinical Code records " + records;
                throw new TransformException(msg, t);
            }

            return null;
        }
    }
}
