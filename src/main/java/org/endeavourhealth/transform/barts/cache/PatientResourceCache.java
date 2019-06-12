package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.*;
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
import java.util.UUID;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private final BartsCsvHelper csvHelper;

    //changed both of these to use UUID rather than person ID as its possible for two person IDs to go to the same UUID if they've merged
    private ResourceCache<UUID, PatientBuilder> patientBuildersByPatientUuid = new ResourceCache<>();
    private Set<UUID> patientUuidsJustDeleted = new HashSet<>();
    /*private Map<Long, PatientBuilder> patientBuildersByPersonId = new HashMap<>();
    private Set<Long> personIdsJustDeleted = new HashSet<>();*/

    public PatientResourceCache(BartsCsvHelper csvHelper) {
        this.csvHelper = csvHelper;
    }

    public PatientBuilder borrowPatientBuilder(Long personId) throws Exception {
        //if we've only got a number and not a CsvCell, then wrap in a dummy cell
        CsvCell cell = CsvCell.factoryDummyWrapper("" + personId);
        return borrowPatientBuilder(cell);
    }

    private UUID mapPersonIdToUuid(CsvCell personIdCell) throws Exception {
        return IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.Patient, personIdCell.getString());

    }

    public PatientBuilder borrowPatientBuilder(CsvCell personIdCell) throws Exception {

        UUID globallyUniqueId = mapPersonIdToUuid(personIdCell);

        //if we know we've deleted it, return null
        if (patientUuidsJustDeleted.contains(globallyUniqueId)) {
            return null;
        }

        //check the cache
        PatientBuilder cachedResource = patientBuildersByPatientUuid.getAndRemoveFromCache(globallyUniqueId);
        if (cachedResource != null) {
            return cachedResource;
        }

        //check the cache first
        /*PatientBuilder patientBuilder = patientBuildersByPersonId.get(personId);
        if (patientBuilder == null) {*/

        PatientBuilder patientBuilder = null;

        //each of the patient transforms only updates part of the FHIR resource, so we need to retrieve any existing instance to update
        Patient patient = (Patient)csvHelper.retrieveResourceForLocalId(ResourceType.Patient, personIdCell.getString());
        if (patient == null) {
            //if the patient doesn't exist yet, create a new one
            patientBuilder = new PatientBuilder();
            patientBuilder.setId(personIdCell.getString(), personIdCell);

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
            identifierBuilder.setValue(personIdCell.getString(), personIdCell);
        }

        return patientBuilder;
    }

    public void returnPatientBuilder(CsvCell personIdCell, PatientBuilder patientBuilder) throws Exception {
        Long personId = personIdCell.getLong();
        returnPatientBuilder(personId, patientBuilder);
    }

    public void returnPatientBuilder(Long personId, PatientBuilder patientBuilder) throws Exception {
        UUID globallyUniqueId = IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.Patient, personId.toString());
        patientBuildersByPatientUuid.addToCache(globallyUniqueId, patientBuilder);
    }

    public void filePatientResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.info("Saving " + patientBuildersByPatientUuid.size() + " patients to the DB");
        int done = 0;

        for (UUID patientUuid: patientBuildersByPatientUuid.keySet()) {
            PatientBuilder patientBuilder = patientBuildersByPatientUuid.getAndRemoveFromCache(patientUuid);

            boolean performIdMapping = !patientBuilder.isIdMapped();

            /*String json = FhirSerializationHelper.serializeResource(patientBuilder.getResource());
            LOG.trace("Saving " + patientBuilder.getResource().getResourceType() + " " + patientBuilder.getResource().getId() + " map IDs = " + performIdMapping + "\n" + json);*/

            fhirResourceFiler.savePatientResource(null, performIdMapping, patientBuilder);

            done ++;
            if (done % 50000 == 0) {
                LOG.info("Done " + done);
            }
        }

        LOG.info("Finishing saving patients to the DB");

        //clear down as everything has been saved
        patientBuildersByPatientUuid.clear();
    }

    /*public void dumpCacheContents() throws Exception {
        if (!LOG.isTraceEnabled()) {
            return;
        }

        LOG.trace("Dumping patient resource cache, size " + patientBuildersByPatientUuid.size());

        for (UUID patientUuid: patientBuildersByPatientUuid.keySet()) {
            PatientBuilder patientBuilder = patientBuildersByPatientUuid.getAndRemoveFromCache(patientUuid);

            String json = FhirSerializationHelper.serializeResource(patientBuilder.getResource());
            LOG.trace("Got " + patientBuilder.getResource().getResourceType() + " " + patientBuilder.getResource().getId() + "\n" + json);

            patientBuildersByPatientUuid.addToCache(patientUuid, patientBuilder);
        }

    }*/

    public void deletePatient(PatientBuilder patientBuilder, CsvCell personIdCell, FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState) throws Exception {

        //null may end up passed in, so just ignore
        if (patientBuilder == null) {
            return;
        }

        //record that we know it's deleted
        Long personId = personIdCell.getLong();
        UUID globallyUniqueId = IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.Patient, personId.toString());
        patientUuidsJustDeleted.add(globallyUniqueId);

        //remove from the cache
        patientBuildersByPatientUuid.removeFromCache(globallyUniqueId);

        boolean mapIds = !patientBuilder.isIdMapped();
        fhirResourceFiler.deletePatientResource(parserState, mapIds, patientBuilder);
    }

    /**
     * ensures that a person ID we know we'll be processing is pre-cached
     */
    public void preCachePatientBuilder(CsvCell personIdCell) throws Exception {

        //just in case
        if (BartsCsvHelper.isEmptyOrIsZero(personIdCell)) {
            return;
        }

        UUID globallyUniqueId = mapPersonIdToUuid(personIdCell);

        //if we know we've deleted it, return out
        if (patientUuidsJustDeleted.contains(globallyUniqueId)) {
            return;
        }

        //check the cache
        if (patientBuildersByPatientUuid.contains(globallyUniqueId)) {
            return;
        }

        PatientBuilder patientBuilder = borrowPatientBuilder(personIdCell);
        if (patientBuilder != null) {
            returnPatientBuilder(personIdCell, patientBuilder);
        }
    }
}
