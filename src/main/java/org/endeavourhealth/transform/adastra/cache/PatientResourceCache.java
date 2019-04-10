package org.endeavourhealth.transform.adastra.cache;

import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private ResourceCache<String, PatientBuilder> patientBuildersByPersonId = new ResourceCache<>();

    public PatientBuilder getOrCreatePatientBuilder(CsvCell patientIdCell,
                                                                       AdastraCsvHelper csvHelper,
                                                                       FhirResourceFiler fhirResourceFiler) throws Exception {

        //check the cache
        PatientBuilder cachedResource = patientBuildersByPersonId.getAndRemoveFromCache(patientIdCell.getString());
        if (cachedResource != null) {

            //add it back into the cache as many identical patients can exist in same session
            returnPatientBuilder(patientIdCell,cachedResource);
            return cachedResource;
        }

        PatientBuilder patientBuilder = null;

        Patient patient
                = (Patient) csvHelper.retrieveResource(patientIdCell.getString(), ResourceType.Patient, fhirResourceFiler);
        if (patient == null) {
            //if the Patient doesn't exist yet, create a new one using the Patient Id
            patientBuilder = new PatientBuilder();
            patientBuilder.setId(patientIdCell.getString(), patientIdCell);
        } else {
            patientBuilder = new PatientBuilder(patient);
        }

        return patientBuilder;
    }

    public void cleanUpResourceCache() {
        try {
            patientBuildersByPersonId.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }

    public boolean patientInCache(CsvCell patientIdCell) {
        return patientBuildersByPersonId.contains(patientIdCell.getString());
    }

    public void returnPatientBuilder(CsvCell personIdCell, PatientBuilder patientBuilder) throws Exception {
        returnPatientBuilder(personIdCell.getString(), patientBuilder);
    }

    public void returnPatientBuilder(String personId, PatientBuilder patientBuilder) throws Exception {
        patientBuildersByPersonId.addToCache(personId, patientBuilder);
    }
}
