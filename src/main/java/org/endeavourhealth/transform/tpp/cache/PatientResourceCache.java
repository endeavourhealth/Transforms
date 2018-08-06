package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private Map<Long, PatientBuilder> patientBuildersByRowId = new HashMap<>();
    private Map<Long, PatientBuilder> patientBuildersToDelete = new HashMap<>();

    public PatientBuilder getOrCreatePatientBuilder(CsvCell rowIdCell, TppCsvHelper csvHelper) throws Exception {

        Long key = rowIdCell.getLong();

        //if we've already worked out we're going to delete this patient, return null
        if (patientBuildersToDelete.containsKey(key)) {
            return null;
        }

        PatientBuilder patientBuilder = patientBuildersByRowId.get(key);
        if (patientBuilder == null) {

            Patient patient = (Patient)csvHelper.retrieveResource(rowIdCell.getString(), ResourceType.Patient);
            if (patient == null) {
                //if the Patient doesn't exist yet, create a new one
                patientBuilder = new PatientBuilder();
                patientBuilder.setId(rowIdCell.getString(), rowIdCell);
            } else {
                patientBuilder = new PatientBuilder(patient);
            }

            patientBuildersByRowId.put(key, patientBuilder);
        }
        return patientBuilder;
    }

    public void fileResources(FhirResourceFiler fhirResourceFiler) throws Exception {
        for (Long key: patientBuildersByRowId.keySet()) {
            PatientBuilder patientBuilder = patientBuildersByRowId.get(key);
            boolean mapIds = !patientBuilder.isIdMapped();
            fhirResourceFiler.savePatientResource(null, mapIds, patientBuilder);
        }

        patientBuildersByRowId.clear();

        for (Long key: patientBuildersToDelete.keySet()) {
            PatientBuilder patientBuilder = patientBuildersToDelete.get(key);
            boolean mapIds = !patientBuilder.isIdMapped();
            fhirResourceFiler.deletePatientResource(null, mapIds, patientBuilder);
        }

        patientBuildersToDelete.clear();
    }

    public void addToPendingDeletes(CsvCell rowIdCell, PatientBuilder patientBuilder) {
        Long key = rowIdCell.getLong();
        patientBuildersByRowId.remove(key);
        patientBuildersToDelete.put(key, patientBuilder);
    }
}
