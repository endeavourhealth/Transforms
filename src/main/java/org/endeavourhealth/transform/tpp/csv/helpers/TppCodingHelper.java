package org.endeavourhealth.transform.tpp.csv.helpers;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppCtv3LookupDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppCtv3Lookup;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.SnomedCache;
import org.endeavourhealth.transform.common.StringMemorySaver;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Coding;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TppCodingHelper {

    private static TppCtv3LookupDalI tppCtv3LookupRefDal = DalProvider.factoryTppCtv3LookupDal();
    private static Map<StringMemorySaver, StringMemorySaver> hmCtv3CodeToTerm = new ConcurrentHashMap<>();
    private static Map<StringMemorySaver, Long> hmCtv3CodeToSnomedConcept = new ConcurrentHashMap<>();

    /**
     * adds CTV3 and/or Snomed codes to a CodeableConcept.
     * minimum requirement is a CTV3 code or Snomed code (and everything else null)
     */
    public static void addCodes(CodeableConceptBuilder codeableConceptBuilder,
                                CsvCell snomedCodeCell, CsvCell snomedDescCell,
                                CsvCell ctv3CodeCell, CsvCell ctv3DescCell) throws Exception {

        if ((snomedCodeCell == null || TppCsvHelper.isEmptyOrNegative(snomedCodeCell))
                && (ctv3CodeCell == null || ctv3CodeCell.isEmpty())) {
            throw new Exception("Need a CTV3 code or Snomed concept");
        }

        boolean addedSnomed = false;

        //add the snomed code first, if we have one
        if (snomedCodeCell != null
                && !TppCsvHelper.isEmptyOrNegative(snomedCodeCell)) {

            addedSnomed = addSnomedCoding(codeableConceptBuilder, snomedCodeCell.getLong(), snomedCodeCell);
        }

        //add the CTV3 code second, if we have one
        if (ctv3CodeCell != null
                && !ctv3CodeCell.isEmpty()) {

            String code = ctv3CodeCell.getString();

            //if a TPP local code
            if (code.startsWith("Y")) {
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_TPP_CTV3);

            } else {

                //if a proper CTV3 code, see if we can look up a mapped Snomed code if we've not already added one
                if (!addedSnomed) {
                    Long conceptId = lookUpSnomedConceptForCtv3Code(code);
                    if (conceptId != null) {
                        addSnomedCoding(codeableConceptBuilder, conceptId);
                    }
                }

                // add Ctv3 coding
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
            }

            codeableConceptBuilder.setCodingCode(code, ctv3CodeCell);

            String readV3Term = lookUpTppCtv3Term(code);
            codeableConceptBuilder.setCodingDisplay(readV3Term);
        }

        //set the text from one of the text cells if we have one, preferring Snomed if present
        if (snomedDescCell != null
                && !snomedDescCell.isEmpty()) {
            codeableConceptBuilder.setText(snomedDescCell.getString(), snomedDescCell);

        } else if (ctv3DescCell != null
                && !ctv3DescCell.isEmpty()) {
            codeableConceptBuilder.setText(ctv3DescCell.getString(), ctv3DescCell);

        } else {
            //if not term provided, use the term from one of the codings
            //Snomed will always be first, so go from the start and break once we've found one
            CodeableConcept cc = codeableConceptBuilder.getCodeableConcept();
            for (Coding coding : cc.getCoding()) {
                if (coding.hasDisplay()) {
                    codeableConceptBuilder.setText(coding.getDisplay());
                    break;
                }
            }
        }
    }

    private static Long lookUpSnomedConceptForCtv3Code(String ctv3Code) throws Exception {

        StringMemorySaver cacheKey = new StringMemorySaver(ctv3Code);
        Long cached = hmCtv3CodeToSnomedConcept.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(ctv3Code);
        if (snomedCode == null) {
            return null;
        }

        Long ret = Long.valueOf(snomedCode.getConceptCode());
        hmCtv3CodeToSnomedConcept.put(cacheKey, ret);

        //add to the other cache, since the snomed code object already has its term populated too
        //snomed term cache has moved to a separate class, so keep things simple and just skip this
        /*if (!hmSnomedConceptToTerm.containsKey(ret)) {
            String term = snomedCode.getTerm();
            hmSnomedConceptToTerm.put(ret, new StringMemorySaver(term));
        }*/

        return ret;

    }

    private static boolean addSnomedCoding(CodeableConceptBuilder codeableConceptBuilder, Long snomedConceptId, CsvCell... sourceCells) throws Exception {

        String snomedTerm = SnomedCache.lookUpSnomedTermForConcept(snomedConceptId);
        if (snomedTerm == null) {
            return false;
        }

        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
        codeableConceptBuilder.setCodingCode("" + snomedConceptId, sourceCells);
        codeableConceptBuilder.setCodingDisplay(snomedTerm);

        return true;
    }


    /**
     * Lookup code reference from SRCtv3Transformer generated db
     */
    private static String lookUpTppCtv3Term(String ctv3Code) throws Exception {

        StringMemorySaver cacheKey = new StringMemorySaver(ctv3Code);
        StringMemorySaver cached = hmCtv3CodeToTerm.get(cacheKey);
        if (cached != null) {
            return cached.toString();
        }

        TppCtv3Lookup lookup = tppCtv3LookupRefDal.getContentFromCtv3Code(ctv3Code);
        if (lookup == null) {
            throw new TransformException("Failed to look up CTV3 term for code [" + ctv3Code + "]");
        }

        // Add to the cache
        String term = lookup.getCtv3Text();
        hmCtv3CodeToTerm.put(cacheKey, new StringMemorySaver(term));
        return term;
    }

}