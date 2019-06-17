package org.endeavourhealth.transform.enterprise;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.reference.CernerClinicalEventMappingDalI;
import org.endeavourhealth.core.database.dal.reference.models.CernerClinicalEventMap;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Coding;

import javax.xml.crypto.dsig.TransformException;

/**
 * utility object for passing around values for the three code-related fields on the subscriber Observation table
 */
public class ObservationCodeHelper {

    private static CernerClinicalEventMappingDalI referenceDal = DalProvider.factoryCernerClinicalEventMappingDal();

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

    public static ObservationCodeHelper extractCodeFields(CodeableConcept codeableConcept) throws Exception {
        return extractCodeFields(codeableConcept, true);
    }

    public static ObservationCodeHelper extractCodeFields(CodeableConcept codeableConcept, boolean blockCerner) throws Exception {

        if (codeableConcept == null) {
            return null;
        }

        ObservationCodeHelper ret = new ObservationCodeHelper();

        ret.setSnomedConceptId(CodeableConceptHelper.findSnomedConceptId(codeableConcept));

        Coding originalCoding = CodeableConceptHelper.findOriginalCoding(codeableConcept);

        //the above function may now return a snomed Coding if Snomed was the original scheme, but for consistency
        //with how this used to work, if that happens, ignore it
        if (originalCoding != null
                && originalCoding.getSystem().equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_SNOMED_CT)) {
            originalCoding = null;
        }

        ret.setOriginalCode(findAndFormatOriginalCode(originalCoding));

        //add original term too, for easy display of results
        if (codeableConcept.hasText()) {
            ret.setOriginalTerm(codeableConcept.getText());

        } else if (originalCoding != null
                && originalCoding.hasDisplay()) {

            ret.setOriginalTerm(originalCoding.getDisplay());
        }

        //if we don't have a Snomed code and our original code is a Cerner code, then don't send to the subscriber
        //so return null to prevent that
        if (ret.getSnomedConceptId() == null) {
            if (blockCerner && (originalCoding == null
                    || originalCoding.getSystem().equals(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID))) {
                return null;
            }
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
                || system.equals(FhirCodeUri.CODE_SYSTEM_EMIS_CODE)) {
            //there's already a vast amount of Read2 and Emis data in the table, so it's too late
            //to easily prefix this, so just use the raw code
            return originalCoding.getCode();

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_SNOMED_CT)
                || system.equals(FhirCodeUri.CODE_SYSTEM_EMISSNOMED)) {

            //a Snomed coding should never be picked up as an "original" term,
            //so something has gone wrong
            throw new TransformException("Original coding has system " + system);

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_CTV3)) {
            return "CTV3_" + originalCoding.getCode();

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_ICD10)) {
            return "ICD10_" + originalCoding.getCode();

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_OPCS4)) {
            return "OPCS4_" + originalCoding.getCode();

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID)) {
            return "CERNER_" + originalCoding.getCode();

        } else {
            throw new TransformException("Unsupported original code system [" + system + "]");
        }
    }

    public static Long getSnomedFromCerner(CodeableConcept concept) throws Exception {
        //Try to get a SNOMED code mapped from a Barts Cerner value.
        Coding originalCoding = CodeableConceptHelper.findOriginalCoding(concept);
        if (originalCoding != null
                && originalCoding.getSystem().equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID)
                && StringUtils.isNumeric(originalCoding.getCode())) {
            Long codeLong = Long.parseLong(originalCoding.getCode());
            CernerClinicalEventMap mapping = referenceDal.findMappingForCvrefCode(codeLong);
            if (mapping != null) {
                return Long.parseLong(mapping.getSnomedConceptId());
            }
        }
        return null;
    }
}
