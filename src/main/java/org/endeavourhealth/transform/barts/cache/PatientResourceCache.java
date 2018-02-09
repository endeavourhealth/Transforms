package org.endeavourhealth.transform.barts.cache;

import org.hl7.fhir.instance.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
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
}
