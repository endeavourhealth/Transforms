package org.endeavourhealth.transform.terminology;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.reference.Read2ToSnomedMapDalI;
import org.endeavourhealth.core.database.dal.reference.SnomedDalI;
import org.endeavourhealth.core.database.dal.reference.models.Read2ToSnomedMap;
import org.endeavourhealth.core.database.dal.reference.models.SnomedLookup;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Coding;

import java.util.List;

public abstract class TerminologyService {

    private static SnomedDalI snomedRepository = DalProvider.factorySnomedDal();
    private static Read2ToSnomedMapDalI read2ToSnomedRepository = DalProvider.factoryRead2ToSnomedMapDal();

    public static SnomedCode lookupSnomedFromConceptId(String conceptId) throws Exception {
        SnomedLookup snomedLookup = snomedRepository.getSnomedLookup(conceptId);

        return new SnomedCode(conceptId, snomedLookup.getTerm());
    }

    public static SnomedCode translateRead2ToSnomed(String code) throws Exception {
        //1)  get conceptId from Read2/Snomed map table
        Read2ToSnomedMap read2ToSnomedMap = read2ToSnomedRepository.getRead2ToSnomedMap(code);

        //2) get Snomed term from lookup table
        String conceptId = read2ToSnomedMap.getConceptId();
        SnomedLookup snomedLookup = snomedRepository.getSnomedLookup(conceptId);

        return new SnomedCode(conceptId, snomedLookup.getTerm());
    }
    public static SnomedCode translateCtv3ToSnomed(String code) {
        //TODO - terminology service needs completing
        return null;
    }
    public static SnomedCode translateEmisSnomedToSnomed(String code) {
        //TODO - terminology service needs completing
        return null;
    }
    public static SnomedCode translateEmisPreparationToSnomed(String code) {
        //TODO - terminology service needs completing
        return null;
    }

    /**
     * checks the first Coding element in the CodeableConcept and adds a second Coding if it
     * needs to be mapped to SNOMED CT
     */
    public static void translateToSnomed(CodeableConcept codeableConcept) throws TransformException {
        List<Coding> codingList = codeableConcept.getCoding();
        if (codingList.isEmpty()) {
            return;
        }

        try {
            Coding coding = codingList.get(0);
            String system = coding.getSystem();
            if (system.equals(FhirUri.CODE_SYSTEM_SNOMED_CT)) {
                //no mapping required unless no display term present
                if (Strings.isNullOrEmpty(coding.getDisplay())) {
                    SnomedCode mapping = TerminologyService.lookupSnomedFromConceptId(coding.getCode());
                    codeableConcept.addCoding(mapping.toCoding());
                }
            } else if (system.equals(FhirUri.CODE_SYSTEM_CTV3)) {
                SnomedCode mapping = TerminologyService.translateCtv3ToSnomed(coding.getCode());
                codeableConcept.addCoding(mapping.toCoding());
            } else if (system.equals(FhirUri.CODE_SYSTEM_READ2)) {
                SnomedCode mapping = TerminologyService.translateRead2ToSnomed(coding.getCode());
                codeableConcept.addCoding(mapping.toCoding());
            } else if (system.equals(FhirUri.CODE_SYSTEM_EMISPREPARATION)) {
                SnomedCode mapping = TerminologyService.translateEmisPreparationToSnomed(coding.getCode());
                codeableConcept.addCoding(mapping.toCoding());
            } else if (system.equals(FhirUri.CODE_SYSTEM_EMISSNOMED)) {
                SnomedCode mapping = TerminologyService.translateEmisSnomedToSnomed(coding.getCode());
                codeableConcept.addCoding(mapping.toCoding());
            } else {
                throw new TransformException("Unexpected coding system [" + system + "]");
            }
        }
        catch (Exception e) {
            throw new TransformException("Code Translation Exception", e);
        }
    }
}

