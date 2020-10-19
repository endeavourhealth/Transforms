package org.endeavourhealth.transform.homertonhi.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private ResourceCache<String, PatientBuilder> patientBuildersByPersonEmpiId = new ResourceCache<>();

    public PatientBuilder getPatientBuilder(CsvCell personEmpiIdCell, HomertonHiCsvHelper csvHelper) throws Exception {

        String personEmpiId = personEmpiIdCell.getString();

        //check the cache
        PatientBuilder cachedResource = patientBuildersByPersonEmpiId.getAndRemoveFromCache(personEmpiId);
        if (cachedResource != null) {
            return cachedResource;
        }

        PatientBuilder patientBuilder = null;

        //each of the patient transforms only updates part of the FHIR resource, so we need to retrieve any existing instance to update
        Patient patient = (Patient)csvHelper.retrieveResourceForLocalId(ResourceType.Patient, personEmpiId);
        if (patient == null) {
            //if the patient doesn't exist yet, create a new one
            patientBuilder = new PatientBuilder();
            patientBuilder.setId(personEmpiId, personEmpiIdCell);

        } else {

            patientBuilder = new PatientBuilder(patient);
        }

        return patientBuilder;
    }

    public void returnPatientBuilder(CsvCell personEmpiIdCell, PatientBuilder patientBuilder) throws Exception {
        String personEmpiId = personEmpiIdCell.getString();
        returnPatientBuilder(personEmpiId, patientBuilder);
    }

    public void returnPatientBuilder(String personEmpiId, PatientBuilder patientBuilder) throws Exception {
        patientBuildersByPersonEmpiId.addToCache(personEmpiId, patientBuilder);
    }

    public void filePatientResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.trace("Saving " + patientBuildersByPersonEmpiId.size() + " patients to the DB");

        for (String personEmpiId: patientBuildersByPersonEmpiId.keySet()) {
            PatientBuilder patientBuilder = patientBuildersByPersonEmpiId.getAndRemoveFromCache(personEmpiId);

            boolean performIdMapping = !patientBuilder.isIdMapped();
            fhirResourceFiler.savePatientResource(null, performIdMapping, patientBuilder);
        }

        //clear down as everything has been saved
        patientBuildersByPersonEmpiId.clear();
    }

    public void deletePatient(PatientBuilder patientBuilder, CsvCell personEmpiIdCell, FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState) throws Exception {

        //null may end up passed in, so just ignore
        if (patientBuilder == null) {
            return;
        }

        //remove from the cache
        patientBuildersByPersonEmpiId.removeFromCache(personEmpiIdCell.getString());

        boolean mapIds = !patientBuilder.isIdMapped();
        fhirResourceFiler.deletePatientResource(parserState, mapIds, patientBuilder);
    }

    /**
     * if we have had an error that's caused us to drop out of the transform, we can call this to tidy up
     * anything we've saved to the audit.queued_message table
     */
    public void cleanUpResourceCache() {
        try {
            patientBuildersByPersonEmpiId.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }
}
