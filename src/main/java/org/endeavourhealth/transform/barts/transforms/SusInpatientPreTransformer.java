package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.SusPatientCache;
import org.endeavourhealth.transform.barts.cache.SusPatientCacheEntry;
import org.endeavourhealth.transform.barts.schema.SusInpatient;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SusInpatientPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SusOutpatientTailPreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {
        SusPatientCache cache = csvHelper.getSusPatientCache();
        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((org.endeavourhealth.transform.barts.schema.SusInpatient) parser, csvHelper, cache);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
        LOG.info("SusPatient cache size:" + cache.size());

    }

    private static void processRecord(SusInpatient parser, BartsCsvHelper csvHelper, SusPatientCache cache) {

        //only cache the fields we know we'll need
        if (!parser.getProcedureSchemeInUse().equals(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
            return;
        }
        CsvCell cdsUniqueId = parser.getCdsUniqueId();
        if (!csvHelper.getSusPatientTailCache().CSDuniqueIdInCache(cdsUniqueId.getString())) {
            LOG.warn("Inpatient records with CdsUniqueId " + cdsUniqueId.getString() + " has no tail record. ");
        }
        CsvCell localPatientId = parser.getLocalPatientId();
        CsvCell nhsNumber = parser.getNhsNumber();
//        CsvCell cdsActivityDate = parser.getCdsActivityDate();
        CsvCell primaryProcedureOPCS = parser.getPrimaryProcedureOPCS();
        CsvCell primaryProcedureDate = parser.getPrimaryProcedureDate();
        CsvCell secondaryProcedureOPCS = parser.getSecondaryProcedureOPCS();
        CsvCell secondaryProcedureDate = parser.getSecondaryProcedureDate();
        CsvCell otherProcedureOPCS = parser.getAdditionalecondaryProceduresOPCS();
        List<String> otherProcs = new ArrayList<>();
        for (String word : otherProcedureOPCS.getString().split(" ")) {
            otherProcs.add(word);
        }

        SusPatientCacheEntry obj = new SusPatientCacheEntry();
        obj.setCDSUniqueIdentifier(cdsUniqueId);
        obj.setLocalPatientId(localPatientId);
        obj.setNHSNumber(nhsNumber);
        //      obj.setCdsActivityDate(cdsActivityDate);
        obj.setPrimaryProcedureOPCS(primaryProcedureOPCS);
        obj.setPrimaryProcedureDate(primaryProcedureDate);
        obj.setSecondaryProcedureOPCS(secondaryProcedureOPCS);
        obj.setSecondaryProcedureDate(secondaryProcedureDate);
        obj.setOtherSecondaryProceduresOPCS(otherProcedureOPCS);

        cache.cacheRecord(obj);
    }
}
