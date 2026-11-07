package org.endeavourhealth.transform.enterprise;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.hl7.fhir.instance.model.Coding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemSchemeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SystemSchemeHelper.class);

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
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_IMPERIAL_CERNER_CODE_ID)) {
            str = IMConstant.IMPERIAL_CERNER;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_CARE_CONNECT_LANGUAGE )) {
            str = IMConstant.FHIR_LANGUAGE;
        }else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_CM_DISCOVERY_CODE )) {
            str = IMConstant.DISCOVERY_CODE;
        } else if (codingSystem.equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_TPP_DRUG_ACTION_GROUP)) {
            //no support in IM for Action Groups
            str = null;

        } else {
            //confirmed that the IM does not support throwing raw URLs at it, so if we don't match
            //any of the above something is very wrong
            throw new Exception("No mapping to IM scheme for code scheme " + codingSystem);
        }

        return str;
    }

    public static String mapImSchemeToCodingSystem(String imScheme) throws Exception {
        String systemCode=null;
        if(imScheme.equalsIgnoreCase(IMConstant.DISCOVERY_CODE)) {
            systemCode = FhirCodeUri.CODE_SYSTEM_CM_DISCOVERY_CODE;
        } else if (imScheme.equalsIgnoreCase(IMConstant.FHIR_LANGUAGE )) {
            systemCode = FhirCodeUri.CODE_SYSTEM_CARE_CONNECT_LANGUAGE;
        } else if (imScheme.equalsIgnoreCase(IMConstant.IMPERIAL_CERNER)) {
            systemCode = FhirCodeUri.CODE_SYSTEM_IMPERIAL_CERNER_CODE_ID;
        }
        else {
            throw new Exception("No mapping to code scheme for IM scheme " + imScheme);
        }
        return systemCode;
    }
}

