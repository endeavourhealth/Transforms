package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private ResourceCache<Long, PatientBuilder> patientBuildersByPersonId = new ResourceCache<>();
    //private Map<Long, PatientBuilder> patientBuildersByPersonId = new HashMap<>();
    private Set<Long> personIdsJustDeleted = new HashSet<>();



    public PatientBuilder borrowPatientBuilder(Long personId, BartsCsvHelper csvHelper) throws Exception {
        //if we've only got a number and not a CsvCell, then wrap in a dummy cell
        CsvCell cell = CsvCell.factoryDummyWrapper("" + personId);
        return borrowPatientBuilder(cell, csvHelper);
    }

    public PatientBuilder borrowPatientBuilder(CsvCell personIdCell, BartsCsvHelper csvHelper) throws Exception {

        Long personId = personIdCell.getLong();

        //if we know we've deleted it, return null
        if (personIdsJustDeleted.contains(personId)) {
            return null;
        }

        //check the cache
        PatientBuilder cachedResource = patientBuildersByPersonId.getAndRemoveFromCache(personId);
        if (cachedResource != null) {
            return cachedResource;
        }

        //check the cache first
        /*PatientBuilder patientBuilder = patientBuildersByPersonId.get(personId);
        if (patientBuilder == null) {*/

        PatientBuilder patientBuilder = null;

        //each of the patient transforms only updates part of the FHIR resource, so we need to retrieve any existing instance to update
        Patient patient = (Patient)csvHelper.retrieveResourceForLocalId(ResourceType.Patient, personId.toString());
        if (patient == null) {
            //if the patient doesn't exist yet, create a new one
            patientBuilder = new PatientBuilder();
            patientBuilder.setId(personId.toString(), personIdCell);

        } else {
            patientBuilder = new PatientBuilder(patient);
        }

        //always set the managing organisation to Barts if not set
        if (!patientBuilder.hasManagingOrganisation()) {
            String bartsId = csvHelper.findOrgRefIdForBarts();
            Reference organisationReference = ReferenceHelper.createReference(ResourceType.Organization, bartsId);
            patientBuilder.setManagingOrganisation(organisationReference);
        }

        //and always ensure we've got the Person ID on the resource
        List<Identifier> personIdIdentifiers = IdentifierBuilder.findExistingIdentifiersForSystem(patientBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_INTERNAL_PERSON);
        if (personIdIdentifiers.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_INTERNAL_PERSON);
            identifierBuilder.setValue(personId.toString(), personIdCell);
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
            //PatientBuilder patientBuilder = patientBuildersByPersonId.get(personId);

            boolean performIdMapping = !patientBuilder.isIdMapped();
            fhirResourceFiler.savePatientResource(null, performIdMapping, patientBuilder);
        }

        LOG.trace("Finishing saving patients to the DB");

        //clear down as everything has been saved
        patientBuildersByPersonId.clear();
    }

    public void deletePatient(PatientBuilder patientBuilder, CsvCell personIdCell, FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState) throws Exception {

        //null may end up passed in, so just ignore
        if (patientBuilder == null) {
            return;
        }

        //record that we know it's deleted
        Long personId = personIdCell.getLong();
        personIdsJustDeleted.add(personId);

        //remove from the cache
        patientBuildersByPersonId.removeFromCache(personId);
        //patientBuildersByPersonId.remove(personId);

        boolean mapIds = !patientBuilder.isIdMapped();
        fhirResourceFiler.deletePatientResource(parserState, mapIds, patientBuilder);
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
