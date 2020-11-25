package org.endeavourhealth.transform.enterprise;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.reference.CernerClinicalEventMappingDalI;
import org.endeavourhealth.core.database.dal.reference.models.CernerClinicalEventMap;
import org.endeavourhealth.core.terminology.Read2Code;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Coding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.dsig.TransformException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * utility object for passing around values for the three code-related fields on the subscriber Observation table
 */
public class ObservationCodeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(ObservationCodeHelper.class);

    private static CernerClinicalEventMappingDalI referenceDal = DalProvider.factoryCernerClinicalEventMappingDal();
    private static Map<Long, Long> hmCernerSnomedMapCache = new HashMap<>();
    private static ReentrantLock cacheLock = new ReentrantLock();

    private Long snomedConceptId = null;
    private String originalCode = null;
    private String originalTerm = null;

    public Long getSnomedConceptId() {
        return snomedConceptId;
    }

    public void setSnomedConceptId(Long snomedConceptId) {
        this.snomedConceptId = snomedConceptId;
    }

    public String getOriginalCode() {
        return originalCode;
    }

    public void setOriginalCode(String originalCode) {
        this.originalCode = originalCode;
    }

    public String getOriginalTerm() {
        return originalTerm;
    }

    public void setOriginalTerm(String originalTerm) {
        this.originalTerm = originalTerm;
    }

    public static Coding findOriginalCoding(CodeableConcept codeableConcept) throws Exception {
        Coding ret = CodeableConceptHelper.findOriginalCoding(codeableConcept);

        //https://endeavourhealth.atlassian.net/browse/SD-130
        //massive hack to get around incomplete FHIR data - if it's a Read2 code missing its display
        //term then look it up
        if (ret != null
                && ret.getSystem().equals(FhirCodeUri.CODE_SYSTEM_READ2)
                && ret.hasCode()
                && !ret.hasDisplay()) {
            String code = ret.getCode();
            Read2Code lookup = TerminologyService.lookupRead2Code(code);
            if (lookup != null) {
                String term = lookup.getPreferredTerm();
                ret.setDisplay(term);
                LOG.debug("Had to look up " + term + " for Read2 code " + code + " (see SD-130)");

            } else {
                LOG.warn("Failed to find term for Read2 code " + code + " (see SD-192)");
                return null;
            }
        }

        //when IM concepts are looked up it requires a term as well as a code, but some suppliers don't give
        //terms in all cases. If the Coding doesn't have a term, see if the CodeableConcept text can be used, and
        //if not return null since there's not sufficient data to use.
        if (ret != null
                && ret.hasCode()
                && !ret.hasDisplay()) {

            if (codeableConcept.hasText()) {
                String text = codeableConcept.getText();
                ret.setDisplay(text);
                LOG.debug("Had to use CodeableConcept text for coding " + ret.getSystem() + " [" + ret.getCode() + "]");

            } else {
                LOG.warn("Failed to find any term for original coding " + ret.getSystem() + " [" + ret.getCode() + "]");
                return null;
            }
        }

        return ret;
    }

    public static ObservationCodeHelper extractCodeFields(CodeableConcept codeableConcept) throws Exception {
        if (codeableConcept == null) {
            return null;
        }

        ObservationCodeHelper ret = new ObservationCodeHelper();

        Long snomedConceptId = CodeableConceptHelper.findSnomedConceptId(codeableConcept);
        if (snomedConceptId != null) {
            ret.setSnomedConceptId(snomedConceptId);
        }

        Coding originalCoding = findOriginalCoding(codeableConcept);

        //the above function may now return a snomed Coding if Snomed was the original scheme, but for consistency
        //with how this used to work, if that happens, ignore it
        if (originalCoding != null
                && originalCoding.getSystem().equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_SNOMED_CT)) {
            originalCoding = null;
        }

        String formattedCode = findAndFormatOriginalCode(originalCoding);
        if (formattedCode != null) {
            ret.setOriginalCode(formattedCode);
        }

        //add original term too, for easy display of results
        if (codeableConcept.hasText()) {
            ret.setOriginalTerm(codeableConcept.getText());

        } else if (originalCoding != null
                && originalCoding.hasDisplay()) {

            ret.setOriginalTerm(originalCoding.getDisplay());
        }

        //if we've not got a Snomed code and our original code was a Cerner code, then see if can map to a Snomed concept
        if (ret.getSnomedConceptId() == null
                && originalCoding != null
                && originalCoding.getSystem().equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_BARTS_CERNER_CODE_ID)) {

            Long mappedSnomedConceptId = mapCernerCodeToSnomed(originalCoding);
            if (mappedSnomedConceptId != null) {
                ret.setSnomedConceptId(mappedSnomedConceptId);
            }
        }

        //if there's neither code, return null
        if (ret.getSnomedConceptId() == null
                && ret.getOriginalCode() == null) {
            return null;
        }

        return ret;
    }


    /**
     * we have the original code column to tell us what the non-snomed code was in the source system,
     * but need to unambiguously know what coding scheme that was, so we prefix the original code
     * with a short string to say what it was
     */
    public static String findAndFormatOriginalCode(Coding originalCoding) throws Exception {

        if (originalCoding == null) {
            return null;
        }

        String system = originalCoding.getSystem();
        if (system.equals(FhirCodeUri.CODE_SYSTEM_READ2)
                || system.equals(FhirCodeUri.CODE_SYSTEM_EMIS_CODE)
                || system.equals(FhirCodeUri.CODE_SYSTEM_VISION_CODE)) {
            //there's already a vast amount of Read2 and Emis data in the table, so it's too late
            //to easily prefix this, so just use the raw code
            return originalCoding.getCode();

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_SNOMED_CT)
                || system.equals(FhirCodeUri.CODE_SYSTEM_EMISSNOMED)) {

            //a Snomed coding should never be picked up as an "original" term,
            //so something has gone wrong
            throw new TransformException("Original coding has system " + system);

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_CTV3)
                || system.equals(FhirCodeUri.CODE_SYSTEM_TPP_CTV3)) {
            return "CTV3_" + originalCoding.getCode();

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_ICD10)) {
            return "ICD10_" + originalCoding.getCode();

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_OPCS4)) {
            return "OPCS4_" + originalCoding.getCode();

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_BARTS_CERNER_CODE_ID)) {
            //changing prefix for Cerner to "Barts Cerner" (BC) as not all Cerners are the same
            return "BC_" + originalCoding.getCode();
            //return "CERNER_" + originalCoding.getCode();

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_TPP_DRUG_ACTION_GROUP)) {
            return "TPPAG_" + originalCoding.getCode();

        } else {
            throw new TransformException("Unsupported original code system [" + system + "]");
        }
    }


    private static Long mapCernerCodeToSnomed(Coding originalCoding) throws Exception {

        //Try to get a SNOMED code mapped from a Barts Cerner value.
        if (originalCoding == null
                || !originalCoding.getSystem().equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_BARTS_CERNER_CODE_ID)
                || !StringUtils.isNumeric(originalCoding.getCode())) {
            return null;
        }

        Long codeLong = Long.valueOf(originalCoding.getCode());

        //check the cache - note we cache lookup failures too (as null values)
        try {
            cacheLock.lock();

            if (hmCernerSnomedMapCache.containsKey(codeLong)) {
                return hmCernerSnomedMapCache.get(codeLong);
            }
        } finally {
            cacheLock.unlock();
        }

        //hit the DB
        CernerClinicalEventMap mapping = referenceDal.findMappingForCvrefCode(codeLong);

        //add to the cache - cache lookup failures too
        try {
            cacheLock.lock();

            if (mapping != null) {
                Long val = Long.valueOf(mapping.getSnomedConceptId());
                hmCernerSnomedMapCache.put(codeLong, val);
                return val;
            } else {
                hmCernerSnomedMapCache.put(codeLong, null);
                return null;
            }

        } finally {
            cacheLock.unlock();
        }
    }



    public static String mapCodingSystemToImScheme(Coding coding) throws Exception {
        return mapCodingSystemToImScheme(coding.getSystem());
    }

    public static String mapCodingSystemToImScheme(String codingSystem) throws Exception {
        String str = null;
        if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_SNOMED_CT)) {
            str = IMConstant.SNOMED;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_READ2)) {
            str = IMConstant.READ2;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_CTV3)) {
            str = IMConstant.CTV3;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_ICD10)) {
            str = IMConstant.ICD10;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_OPCS4)) {
            str = IMConstant.OPCS4;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_BARTS_CERNER_CODE_ID)) {
            str = IMConstant.BARTS_CERNER;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_EMIS_CODE)) {
            str = IMConstant.EMIS_LOCAL;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_VISION_CODE)) {
            str = IMConstant.VISION_LOCAL;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_TPP_CTV3)) {
            str = IMConstant.TPP_LOCAL;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_TPP_DRUG_ACTION_GROUP)) {
            //no support in IM for Action Groups
            str = null;

        } else {
            //confirmed that the IM does not support throwing raw URLs at it, so if we don't match
            //any of the above something is very wrong
            throw new Exception("No mapping to IM scheme for code scheme " + codingSystem);
            //str = codingSystem;
        }

        return str;
    }
}
