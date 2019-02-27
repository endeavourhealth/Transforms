package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.SusPatientCache;
import org.endeavourhealth.transform.barts.cache.SusPatientCacheEntry;
import org.endeavourhealth.transform.barts.schema.SusInpatient;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;

import java.util.ArrayList;
import java.util.List;

public class SusInpatientPreTransformer {

       public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((org.endeavourhealth.transform.barts.schema.SusInpatient)parser, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

    }
    private static void processRecord(SusInpatient parser, BartsCsvHelper csvHelper) {
        SusPatientCache cache = csvHelper.getSusPatientCache();
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
        cache.cacheRecord(obj);
    }
}
