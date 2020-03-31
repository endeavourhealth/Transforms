package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.CoreFilerDalI;
import org.endeavourhealth.core.database.dal.ehr.models.CoreFilerWrapper;
import org.endeavourhealth.core.database.dal.ehr.models.CoreId;
import org.endeavourhealth.core.database.dal.ehr.models.CoreTableId;
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

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer.PRIMARY_ORG_ODS_CODE;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private final BartsCsvHelper csvHelper;

    //changed both of these to use UUID rather than person ID as its possible for two person IDs to go to the same UUID if they've merged
    private ResourceCache<UUID, PatientBuilder> patientBuildersByPatientUuid = new ResourceCache<>();
    private Set<UUID> patientUuidsJustDeleted = new HashSet<>();

    private Map<Integer, org.endeavourhealth.core.database.dal.ehr.models.Patient> patientV2Instances = new ConcurrentHashMap<>();
    private static CoreFilerDalI repository = DalProvider.factoryCoreFilerDal();

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

    public org.endeavourhealth.core.database.dal.ehr.models.Patient borrowPatientV2Instance(CsvCell personIdCell) throws Exception {

        CoreId corePatientId = csvHelper.getCoreId(CoreTableId.PATIENT.getId(), personIdCell.getString());
        Integer patientId = corePatientId.getCoreId();

        org.endeavourhealth.core.database.dal.ehr.models.Patient cachedPatient = patientV2Instances.get(patientId);
        if (cachedPatient != null) {
            return cachedPatient;   //found in cache so return that instance
        }

        //try db lookup
        org.endeavourhealth.core.database.dal.ehr.models.Patient patient
                = csvHelper.getCorePatient(patientId);
        if (patient == null) {

            patient = new org.endeavourhealth.core.database.dal.ehr.models.Patient();
            patient.setId(patientId);

            //create the personId reference using the same sourceId used for the patientId
            CoreId corePersonId = csvHelper.getCoreId(CoreTableId.PERSON.getId(), personIdCell.getString());
            Integer personId = corePersonId.getCoreId();
            patient.setPersonId(personId);

            Integer organizationId = csvHelper.findOrganizationIdForOds(PRIMARY_ORG_ODS_CODE);
            patient.setOrganizationId(organizationId);

        } else {

            //add to cache
            patientV2Instances.put(patientId, patient);
            return patient;
        }
        return patient;
    }

    public void returnPatientV2Instance(CsvCell personIdCell, org.endeavourhealth.core.database.dal.ehr.models.Patient patient) throws Exception {

        CoreId corePatientId = csvHelper.getCoreId(CoreTableId.PATIENT.getId(), personIdCell.getString());
        patientV2Instances.put(corePatientId.getCoreId(), patient);
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

    private static void savePatientV2Batch(List<CoreFilerWrapper> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

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
