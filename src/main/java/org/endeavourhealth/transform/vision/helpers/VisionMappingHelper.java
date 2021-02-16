package org.endeavourhealth.transform.vision.helpers;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.ResourceParser;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Patient;

import java.util.Map;

public class VisionMappingHelper {

    private static Map<String, String> ethnicityMap;
    private static Map<String, String> maritalStatusValueMap;
    private static Map<String, String> maritalStatusRead2Map;
    private static Map<String, String> maritalStatusTermMap;
    private static Map<String, String> maritalStatusSnomedMap;


    /**
     * Vision give no indication of what "type" of code Journal records are, so we just need to infer
     * it from the specific branch. The below list of branches was derived from the Emis code file
     * which DOES define which codes are ethnicities
     */
    public static boolean isPotentialEthnicity(String read2Code) {
        return read2Code.startsWith("134") //Country of origin
            || read2Code.startsWith("9i") //Ethnic category - 2001 census
            || read2Code.startsWith("9S") //Ethnic groups (census)
            || read2Code.startsWith("9T") //Ethnicity and other related nationality data
            || read2Code.startsWith("9t"); //Ethnic category - 2011 census
    }

    public static EthnicCategory findEthnicityCode(String read2Code) throws Exception {

        if (Strings.isNullOrEmpty(read2Code)) {
            return null;
        }

        if (ethnicityMap == null) {
            ethnicityMap = ResourceParser.readCsvResourceIntoMap("VisionEthnicityMap.csv", "SourceCode", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }

        String code = ethnicityMap.get(read2Code);
        if (code == null) {
            throw new RuntimeException("Unknown ethnicity code " + read2Code);

        } else if (!Strings.isNullOrEmpty(code)) {
            return EthnicCategory.fromCode(code);

        } else {
            //if mapped to a an empty string, just return null
            return null;
        }
    }

    public static void applyEthnicityCode(VisionCsvHelper.DateAndEthnicityCategory newEthnicity, PatientBuilder patientBuilder) {
        if (newEthnicity == null) {
            return;
        }

        EthnicCategory ethnicCategory = newEthnicity.getEthnicCategory(); //note this might be null if it's an ethnicity code we can't map
        CsvCell sourceCell = newEthnicity.getSourceCell();
        patientBuilder.setEthnicity(ethnicCategory, sourceCell);
    }

    public static MaritalStatus mapMaritalStatusToValueSet(CsvCell maritalStatusCell) throws Exception {
        if (maritalStatusCell.isEmpty()) {
            return null;
        }

        if (maritalStatusValueMap == null) {
            maritalStatusValueMap = ResourceParser.readCsvResourceIntoMap("VisionMaritalStatusMap.csv", "SourceCode", "MappedMaritalStatusCode", CSVFormat.DEFAULT.withHeader());
        }

        String key = maritalStatusCell.getString();
        String value = maritalStatusValueMap.get(key);
        if (value == null) {
            throw new RuntimeException("Unknown marital status code " + key);

        } else if (!Strings.isNullOrEmpty(value)) {
            return MaritalStatus.fromCode(value);

        } else {
            //if mapped to a an empty string, just return null
            return null;
        }
    }

    public static String mapMaritalStatusReadCode(CsvCell maritalStatusCell) throws Exception {
        if (maritalStatusCell.isEmpty()) {
            return null;
        }

        if (maritalStatusRead2Map == null) {
            maritalStatusRead2Map = ResourceParser.readCsvResourceIntoMap("VisionMaritalStatusMap.csv", "SourceCode", "MappedRead2Code", CSVFormat.DEFAULT.withHeader());
        }

        String key = maritalStatusCell.getString();
        String value = maritalStatusRead2Map.get(key);
        if (value == null) {
            throw new RuntimeException("Unknown marital status code " + key);
        }
        return value;
    }

    public static String mapMaritalStatusTerm(CsvCell maritalStatusCell) throws Exception {
        if (maritalStatusCell.isEmpty()) {
            return null;
        }

        if (maritalStatusTermMap == null) {
            maritalStatusTermMap = ResourceParser.readCsvResourceIntoMap("VisionMaritalStatusMap.csv", "SourceCode", "MappedTerm", CSVFormat.DEFAULT.withHeader());
        }

        String key = maritalStatusCell.getString();
        String value = maritalStatusTermMap.get(key);
        if (value == null) {
            throw new RuntimeException("Unknown marital status code " + key);
        }
        return value;
    }

    public static String mapMaritalStatusSnomedCode(CsvCell maritalStatusCell) throws Exception {
        if (maritalStatusCell.isEmpty()) {
            return null;
        }

        if (maritalStatusSnomedMap == null) {
            maritalStatusSnomedMap = ResourceParser.readCsvResourceIntoMap("VisionMaritalStatusMap.csv", "SourceCode", "MappedSnomedCode", CSVFormat.DEFAULT.withHeader());
        }

        String key = maritalStatusCell.getString();
        String value = maritalStatusSnomedMap.get(key);
        if (value == null) {
            throw new RuntimeException("Unknown marital status code " + key);
        }
        return value;
    }


}
