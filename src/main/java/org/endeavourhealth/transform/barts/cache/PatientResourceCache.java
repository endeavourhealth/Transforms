package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.transform.common.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.hl7.fhir.instance.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);


    private static Map<Long, Patient> patientResources = new HashMap<>();

    public static void savePatientResource(Long millenniumID, Patient fhirResource) throws Exception {
        patientResources.put(millenniumID, fhirResource);
    }

    public static Patient getPatientResource(Long millenniumId) throws Exception {
        return patientResources.get(millenniumId);
    }

    private static void filePatientResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCurrentState parserState = new CsvCurrentState("PPATI", 0);

        Iterator<Map.Entry<Long,Patient>> iter = patientResources.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long,Patient> entry = iter.next();
            Patient fhirPatient = entry.getValue();
            BasisTransformer.savePatientResource(fhirResourceFiler, parserState, fhirPatient.getId().toString(), fhirPatient);
                iter.remove();

        }

    }
}
