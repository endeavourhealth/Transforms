package org.endeavourhealth.transform.tpp.csv.helpers.cache;

import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * multiple TPP files feed into FHIR Patient resources, so this caches PaitentBuilders in memory
 * until all files are processed, then saves everything
 */
public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private ResourceCache<Long, PatientBuilder> cache = new ResourceCache<>();

    public PatientBuilder borrowPatientBuilder(CsvCell rowIdCell, TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {
        return borrowPatientBuilder(rowIdCell, csvHelper, fhirResourceFiler, false);
    }

    public PatientBuilder borrowPatientBuilder(CsvCell rowIdCell, TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler, boolean createIfNotFound) throws Exception {

        Long key = rowIdCell.getLong();

        PatientBuilder patientBuilder = cache.getAndRemoveFromCache(key);
        if (patientBuilder != null) {
            return patientBuilder;
        }

        //see if our patient has been deleted or doesn't exist and return null if so
        if (!createIfNotFound) {
            if (fhirResourceFiler.isPatientDeleted(rowIdCell.getString())) {
                return null;
            }
        }

        Patient patient = (Patient)csvHelper.retrieveResource(rowIdCell.getString(), ResourceType.Patient);
        if (patient == null) {
            //if the Patient doesn't exist yet, create a new one
            patientBuilder = new PatientBuilder();
            patientBuilder.setId(rowIdCell.getString(), rowIdCell);
        } else {
            patientBuilder = new PatientBuilder(patient);
        }

        return patientBuilder;
    }

    public void returnPatientBuilder(CsvCell patientRowIdCell, PatientBuilder patientBuilder) throws Exception {
        Long key = patientRowIdCell.getLong();
        this.cache.addToCache(key, patientBuilder);
    }

    public void fileResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Long key: cache.keySet()) {
            PatientBuilder patientBuilder = cache.getAndRemoveFromCache(key);

            if (patientBuilder.hasManagingOrganisation() && !patientBuilder.getResource().getId().equals("-1")) {

                try {
                    boolean mapIds = !patientBuilder.isIdMapped();
                    fhirResourceFiler.savePatientResource(null, mapIds, patientBuilder);

                } catch (Exception ex) {
                    LOG.error("Error saving patient " + key);
                    Resource patient = patientBuilder.getResource();
                    String json = FhirSerializationHelper.serializeResource(patient);
                    LOG.error(json);
                    throw ex;
                }
            }
        }

        //won't free up much memory but means nothing can access this class again without causing an exception
        cache = null;
    }


}
