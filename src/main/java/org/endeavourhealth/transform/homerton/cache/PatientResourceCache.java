package org.endeavourhealth.transform.homerton.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private ResourceCache<Long, PatientBuilder> patientBuildersByPersonId = new ResourceCache<>();

    public PatientBuilder getPatientBuilder(CsvCell milleniumPersonIdCell, HomertonCsvHelper csvHelper) throws Exception {

        Long personId = milleniumPersonIdCell.getLong();

        //check the cache
        PatientBuilder cachedResource = patientBuildersByPersonId.getAndRemoveFromCache(personId);
        if (cachedResource != null) {
            return cachedResource;
        }

        PatientBuilder patientBuilder = null;

        //each of the patient transforms only updates part of the FHIR resource, so we need to retrieve any existing instance to update
        Patient patient = (Patient)csvHelper.retrieveResourceForLocalId(ResourceType.Patient, personId.toString());
        if (patient == null) {
            //if the patient doesn't exist yet, create a new one
            patientBuilder = new PatientBuilder();
            patientBuilder.setId(personId.toString());

        } else {

            patientBuilder = new PatientBuilder(patient);
        }

        return patientBuilder;
    }

    public void returnPatientBuilder(CsvCell personIdCell, PatientBuilder patientBuilder) throws Exception {
        Long personId = personIdCell.getLong();
        returnPatientBuilder(personId, patientBuilder);
    }

    public void returnPatientBuilder(Long personId, PatientBuilder patientBuilder) throws Exception {
        patientBuildersByPersonId.addToCache(personId, patientBuilder);
    }

    public void filePatientResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.trace("Saving " + patientBuildersByPersonId.size() + " patients to the DB");

        for (Long personId: patientBuildersByPersonId.keySet()) {
            PatientBuilder patientBuilder = patientBuildersByPersonId.getAndRemoveFromCache(personId);

            boolean performIdMapping = !patientBuilder.isIdMapped();
            fhirResourceFiler.savePatientResource(null, performIdMapping, patientBuilder);
        }

        //clear down as everything has been saved
        patientBuildersByPersonId.clear();
    }

    /**
     * if we have had an error that's caused us to drop out of the transform, we can call this to tidy up
     * anything we've saved to the audit.queued_message table
     */
    public void cleanUpResourceCache() {
        try {
            patientBuildersByPersonId.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }
}
