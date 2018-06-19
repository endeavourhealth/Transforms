package org.endeavourhealth.transform.adastra.cache;

import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private static Map<String, PatientBuilder> PatientBuilderByPatientId = new HashMap<>();

    public static PatientBuilder getOrCreatePatientBuilder(CsvCell patientIdCell,
                                                                       AdastraCsvHelper csvHelper,
                                                                       FhirResourceFiler fhirResourceFiler) throws Exception {

        PatientBuilder patientBuilder = PatientBuilderByPatientId.get(patientIdCell.getString());
        if (patientBuilder == null) {

            Patient patient
                    = (Patient) csvHelper.retrieveResource(patientIdCell.getString(), ResourceType.Patient, fhirResourceFiler);
            if (patient == null) {
                //if the Patient doesn't exist yet, create a new one using the Patient Id
                patientBuilder = new PatientBuilder();
                patientBuilder.setId(patientIdCell.getString(), patientIdCell);
            } else {
                patientBuilder = new PatientBuilder(patient);
            }

            PatientBuilderByPatientId.put(patientIdCell.getString(), patientBuilder);
        }
        return patientBuilder;
    }

    public static int size() {
        return PatientBuilderByPatientId.size();
    }

    public static void clear() {
        PatientBuilderByPatientId.clear();
    }

    public static boolean patientInCache(CsvCell patientIdCell) {
        return PatientBuilderByPatientId.containsKey(patientIdCell.getString());
    }
}
