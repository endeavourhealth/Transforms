package org.endeavourhealth.transform.nwlextracts.cache;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.CoreFilerDalI;
import org.endeavourhealth.core.database.dal.ehr.models.CoreFilerWrapper;
import org.endeavourhealth.core.database.dal.ehr.models.CoreTableId;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private final NwlExtractsCsvHelper csvHelper;

    //changed both of these to use UUID rather than person ID as its possible for two person IDs to go to the same UUID if they've merged
    private ResourceCache<UUID, PatientBuilder> patientBuildersByPatientUuid = new ResourceCache<>();
    private Set<UUID> patientUuidsJustDeleted = new HashSet<>();

    private Map<Integer, org.endeavourhealth.core.database.dal.ehr.models.Patient> patientV2Instances = new ConcurrentHashMap<>();
    private static CoreFilerDalI repository = DalProvider.factoryCoreFilerDal();

    public PatientResourceCache(NwlExtractsCsvHelper csvHelper) {
        this.csvHelper = csvHelper;
    }

    public PatientBuilder borrowPatientBuilder(Long personId, String sourceSystem) throws Exception {
        //if we've only got a number and not a CsvCell, then wrap in a dummy cell
        CsvCell cell = CsvCell.factoryDummyWrapper("" + personId);
        return borrowPatientBuilder(cell,sourceSystem);
    }

    private UUID mapPersonIdToUuid(CsvCell personIdCell) throws Exception {
        return IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.Patient, personIdCell.getString());

    }

    private UUID fetchPersonIdToUuid(CsvCell personIdCell) throws Exception {
        return IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.Patient, personIdCell.getString());
    }

    private UUID assignPersonIdToUuid(CsvCell identifier,UUID edsId) throws Exception {
        return IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.Patient, identifier.getString(),edsId);
    }

    public PatientBuilder borrowPatientBuilder(List<CsvCell> patientIdentifiers, String sourceSystem) throws Exception {

        // Loop through all the identifiers and check for edsid. If found store the edsid and identifier for which edsid is found.
        UUID globallyUniqueId = null;
        CsvCell edsIdMappedIdentifer = null;
        for (CsvCell identifier : patientIdentifiers) {
            globallyUniqueId = fetchPersonIdToUuid(identifier);
            if(globallyUniqueId != null){
                edsIdMappedIdentifer = identifier;
                break;
            }
        }
        // If edsid not found create a edsid for personid.
        CsvCell personIdCell = patientIdentifiers.get(0);
        if(globallyUniqueId == null){
            globallyUniqueId = mapPersonIdToUuid(personIdCell);
            edsIdMappedIdentifer = personIdCell;
        }

        // Map the same edsid to remaining identifiers.
        for (CsvCell identifier : patientIdentifiers) {
            if(identifier != edsIdMappedIdentifer){
                assignPersonIdToUuid(identifier, globallyUniqueId);
            }
        }

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

        //always set the managing organisation to NwlExtracts if not set
        if (!patientBuilder.hasManagingOrganisation()) {
//            String nwlextractsId = csvHelper.findOrgRefIdForNwlExtracts();
            Reference organisationReference = ReferenceHelper.createReference(ResourceType.Organization, sourceSystem);
            patientBuilder.setManagingOrganisation(organisationReference);
        }

        //and always ensure we've got the Person ID on the resource
        List<Identifier> personIdIdentifiers = IdentifierBuilder.findExistingIdentifiersForSystem(patientBuilder, "https://fhir.hl7.org.uk/Id/"+ sourceSystem.toLowerCase());
        if (personIdIdentifiers.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem("https://fhir.hl7.org.uk/Id/"+ sourceSystem.toLowerCase());
            identifierBuilder.setValue(personIdCell.getString(), personIdCell);
        }

        return patientBuilder;
    }


    public PatientBuilder borrowPatientBuilder(CsvCell personIdCell, String sourceSystem) throws Exception {

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

        //always set the managing organisation to NwlExtracts if not set
        if (!patientBuilder.hasManagingOrganisation()) {
            Reference organisationReference = ReferenceHelper.createReference(ResourceType.Organization, sourceSystem);
            patientBuilder.setManagingOrganisation(organisationReference);
        }

        //and always ensure we've got the Person ID on the resource
        List<Identifier> personIdIdentifiers = IdentifierBuilder.findExistingIdentifiersForSystem(patientBuilder, "https://fhir.hl7.org.uk/Id/"+ sourceSystem.toLowerCase());
        if (personIdIdentifiers.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem("https://fhir.hl7.org.uk/Id/"+ sourceSystem.toLowerCase());
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

    public void filePatientV2Instances() throws Exception {

        if (TransformConfig.instance().isLive()) {
            //remove this check for go live
            return;
        }

        LOG.info("Saving " + patientV2Instances.size() + " patient V2 instances to the DB");

        List<CoreFilerWrapper> batch = new ArrayList<>();
        for (int patientId: patientV2Instances.keySet()) {

            org.endeavourhealth.core.database.dal.ehr.models.Patient patient
                    = patientV2Instances.get(patientId);

            //create the CoreFilerWrapper for filing
            CoreFilerWrapper coreFilerWrapper = new CoreFilerWrapper();
            coreFilerWrapper.setServiceId(csvHelper.getServiceId());
            coreFilerWrapper.setSystemId(csvHelper.getSystemId());
            coreFilerWrapper.setDeleted(false);
            coreFilerWrapper.setCreatedAt(new Date());
            coreFilerWrapper.setExchangeId(csvHelper.getExchangeId());
            coreFilerWrapper.setDataType(CoreTableId.PATIENT.getName());
            coreFilerWrapper.setData(patient);
            batch.add(coreFilerWrapper);

            savePatientV2Batch(batch, false, csvHelper);
        }
        savePatientV2Batch(batch, true, csvHelper);
        LOG.info("Finishing saving V2 patients to the DB");

        //clear down as everything has been saved
        patientV2Instances.clear();
    }

    private static void savePatientV2Batch(List<CoreFilerWrapper> batch, boolean lastOne, NwlExtractsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new PatientResourceCache.savePatientV2DataCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    private static class savePatientV2DataCallable implements Callable {

        private List<CoreFilerWrapper> objs = null;
        private UUID serviceId;

        public savePatientV2DataCallable(List<CoreFilerWrapper> objs,
                                UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.save(serviceId, objs);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }


    public void deletePatient(PatientBuilder patientBuilder, CsvCell personIdCell, FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState) throws Exception {

        //null may end up passed in, so just ignore
        if (patientBuilder == null) {
            return;
        }

        //record that we know it's deleted
        UUID globallyUniqueId = mapPersonIdToUuid(personIdCell);
        patientUuidsJustDeleted.add(globallyUniqueId);

        //remove from the cache
        patientBuildersByPatientUuid.removeFromCache(globallyUniqueId);

        boolean mapIds = !patientBuilder.isIdMapped();
        fhirResourceFiler.deletePatientResource(parserState, mapIds, patientBuilder);
    }

    /**
     * ensures that a person ID we know we'll be processing is pre-cached
     */
    public void preCachePatientBuilder(List<CsvCell> patientIdentifiers, String sourceSystem) throws Exception {

        //just in case
        CsvCell personIdCell = patientIdentifiers.get(0);
        if (NwlExtractsCsvHelper.isEmptyOrIsZero(personIdCell)) {
            return;
        }

        UUID globallyUniqueId = fetchPersonIdToUuid(personIdCell);

        if(globallyUniqueId != null){
            //if we know we've deleted it, return out
            if (patientUuidsJustDeleted.contains(globallyUniqueId)) {
                return;
            }

            //check the cache
            if (patientBuildersByPatientUuid.contains(globallyUniqueId)) {
                return;
            }
        }


        PatientBuilder patientBuilder = borrowPatientBuilder(patientIdentifiers,sourceSystem);
        if (patientBuilder != null) {
            returnPatientBuilder(personIdCell, patientBuilder);
        }
    }
    public void preCachePatientBuilder(CsvCell personIdCell, String sourceSystem) throws Exception {

        //just in case
        if (NwlExtractsCsvHelper.isEmptyOrIsZero(personIdCell)) {
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

        PatientBuilder patientBuilder = borrowPatientBuilder(personIdCell, sourceSystem);
        if (patientBuilder != null) {
            returnPatientBuilder(personIdCell, patientBuilder);
        }
    }
}
