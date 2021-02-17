package org.endeavourhealth.transform.tpp.csv.helpers;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.ResourceParser;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;

import java.util.Map;

public class TppMappingHelper {

    private static Map<String, String> ethnicityMap;
    private static Map<String, String> maritalStatusMap;

    public static boolean isEthnicityCode(String ctv3Code) throws Exception {

        if (Strings.isNullOrEmpty(ctv3Code)) {
            return false;
        }

        if (ethnicityMap == null) {
            ethnicityMap = ResourceParser.readCsvResourceIntoMap("TppEthnicityMap.csv", "SourceCode", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }
        
        return ethnicityMap.containsKey(ctv3Code);
    }

    /**
     * SystmOne defines ethnicities as those CTV3 codes being under four specific CTV3 branches (see SD-350 for details)
     * which have been fully expanded and all codes mapped to the NHS Data Dictionary
     */
    public static EthnicCategory findEthnicityCode(String ctv3Code) throws Exception {

        if (Strings.isNullOrEmpty(ctv3Code)) {
            return null;
        }

        if (ethnicityMap == null) {
            ethnicityMap = ResourceParser.readCsvResourceIntoMap("TppEthnicityMap.csv", "SourceCode", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }

        String mappedCode = ethnicityMap.get(ctv3Code);

        //unlike Emis and Vision, we don't have a good way to catch new ethnicity codes being added, since there's no easy way
        //to tell a code is under the specific branches because CTV3 codes don't have the same structure as Read2, where you can
        //tell the hierarchy from the code itself. So we cannot detect ethnicity codes that aren't mapped.
        if (!Strings.isNullOrEmpty(mappedCode)) {
            return EthnicCategory.fromCode(mappedCode);

        } else {
            //if mapped to a an empty string, just return null
            return null;
        }
    }

    public static boolean isMaritalStatusCode(String ctv3Code) throws Exception {

        if (Strings.isNullOrEmpty(ctv3Code)) {
            return false;
        }

        if (maritalStatusMap == null) {
            maritalStatusMap = ResourceParser.readCsvResourceIntoMap("TppMaritalStatusMap.csv", "SourceCode", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }

        return maritalStatusMap.containsKey(ctv3Code);
    }

    /**
     * SystmOne defines ethnicities as those CTV3 codes being under four specific CTV3 branches (see SD-350 for details)
     * which have been fully expanded and all codes mapped to the NHS Data Dictionary
     */
    public static MaritalStatus findMaritalStatusCode(String ctv3Code) throws Exception {

        if (Strings.isNullOrEmpty(ctv3Code)) {
            return null;
        }

        if (maritalStatusMap == null) {
            maritalStatusMap = ResourceParser.readCsvResourceIntoMap("TppMaritalStatusMap.csv", "SourceCode", "TargetCode", CSVFormat.DEFAULT.withHeader());
        }

        String mappedCode = maritalStatusMap.get(ctv3Code);

        //unlike Emis and Vision, we don't have a good way to catch new maritalStatus codes being added, since there's no easy way
        //to tell a code is under the specific branches because CTV3 codes don't have the same structure as Read2, where you can
        //tell the hierarchy from the code itself. So we cannot detect maritalStatus codes that aren't mapped.
        if (!Strings.isNullOrEmpty(mappedCode)) {
            return MaritalStatus.fromCode(mappedCode);

        } else {
            //if mapped to a an empty string, just return null
            return null;
        }
    }

    public static void applyNewEthnicity(TppCsvHelper.DateAndCode newEthnicity, PatientBuilder patientBuilder) {

        if (newEthnicity == null) {
            return;
        }

        CsvCell[] additionalSourceCells = newEthnicity.getAdditionalSourceCells();
        if (newEthnicity.hasCode()) {
            EthnicCategory ethnicCategory = EthnicCategory.fromCode(newEthnicity.getCode());
            patientBuilder.setEthnicity(ethnicCategory, additionalSourceCells);
        } else {
            patientBuilder.setEthnicity(null, additionalSourceCells);
        }
    }

    public static void applyNewMaritalStatus(TppCsvHelper.DateAndCode newMaritalStatus, PatientBuilder patientBuilder) {
        if (newMaritalStatus == null) {
            return;
        }

        CsvCell[] additionalSourceCells = newMaritalStatus.getAdditionalSourceCells();
        if (newMaritalStatus.hasCode()) {
            MaritalStatus maritalStatus = MaritalStatus.fromCode(newMaritalStatus.getCode());
            patientBuilder.setMaritalStatus(maritalStatus, additionalSourceCells);
        } else {
            patientBuilder.setMaritalStatus(null, additionalSourceCells);
        }
    }
}
