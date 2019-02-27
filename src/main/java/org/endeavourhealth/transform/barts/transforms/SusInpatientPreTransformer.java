package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.SusPatientCacheEntry;
import org.endeavourhealth.transform.barts.schema.SusInpatient;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SusInpatientPreTransformer {

    /**
     * simply caches the contents of a tails file into a hashmap
     */
    public static void transform(SusInpatient parser, Map<String, SusPatientCacheEntry> patientCache) throws Exception {

        //don't catch any record level parsing errors, since any problems here need to stop the transform
        while (parser.nextRecord()) {
            processRecord(parser, patientCache);
        }
    }

    private static void processRecord(SusInpatient parser, Map<String, SusPatientCacheEntry> patientCache) {

        //only cache the fields we know we'll need
        if (!parser.getProcedureSchemeInUse().equals(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
            return;
        }
        CsvCell cdsUniqueId = parser.getCdsUniqueId();
        CsvCell localPatientId = parser.getLocalPatientId();
        CsvCell nhsNumber = parser.getNhsNumber();
        CsvCell cdsActivityDate = parser.getCdsActivityDate();
        CsvCell primaryProcedureOPCS = parser.getPrimaryProcedureOPCS();
        CsvCell primaryProcedureDate = parser.getPrimaryProcedureDate();
        CsvCell secondaryProcedureOPCS = parser.getSecondaryProcedureOPCS();
        CsvCell secondaryProcedureDate = parser.getSecondaryProcedureDate();
        CsvCell otherProcedureOPCS = parser.getAdditionalecondaryProceduresOPCS();
        List<String> otherProcs  = new ArrayList<>();
        for(String word : otherProcedureOPCS.getString().split(" ")) {
            otherProcs.add(word);
        }

        SusPatientCacheEntry obj = new SusPatientCacheEntry();
        obj.setCDSUniqueIdentifier(cdsUniqueId);
        obj.setLocalPatientId(localPatientId);
        obj.setNHSNumber(nhsNumber);
        obj.setCdsActivityDate(cdsActivityDate);
        obj.setPrimaryProcedureOPCS(primaryProcedureOPCS);
        obj.setPrimaryProcedureDate(primaryProcedureDate);
        obj.setSecondaryProcedureOPCS(secondaryProcedureOPCS);
        obj.setSecondaryProcedureDate(secondaryProcedureDate);
        obj.setOtherSecondaryProceduresOPCS(otherProcedureOPCS);



        CsvCell uniqueId = parser.getCdsUniqueId();
        patientCache.put(uniqueId.getString(), obj);
    }
}
