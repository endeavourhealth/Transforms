package org.endeavourhealth.transform.common;

import com.datastax.driver.core.utils.UUIDs;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.ServiceDalI;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.database.dal.audit.ExchangeBatchDalI;
import org.endeavourhealth.core.database.dal.audit.models.ExchangeBatch;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherTransform.SourceFileMappingDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.fhirStorage.FhirStorageService;
import org.endeavourhealth.core.xml.TransformErrorUtility;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.endeavourhealth.transform.common.resourceBuilders.GenericBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ResourceBuilderBase;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class FhirResourceFiler implements FhirResourceFilerI {

    private static final Logger LOG = LoggerFactory.getLogger(FhirResourceFiler.class);

    private static Set<ResourceType> patientResourceTypes = null;

    private final long creationTime;
    private final UUID exchangeId;
    private final UUID serviceId;
    private final UUID systemId;
    private final FhirStorageService storageService;
    private final ExchangeBatchDalI exchangeBatchRepository;
    private final TransformError transformError;
    private final List<UUID> batchIdsCreated;

    //batch IDs
    private ReentrantLock batchIdLock = new ReentrantLock();
    private ExchangeBatch adminBatchId = null;
    private Map<UUID, ExchangeBatch> patientBatchIdMap = new ConcurrentHashMap<>();
    private Map<String, UUID> sourcePatientIdMap = new ConcurrentHashMap<>();

    //threading
    private MapIdTask nextMapIdTask = new MapIdTask();
    private ThreadPool threadPoolIdMapper = null;
    private ThreadPool threadPoolFiler = null;

    //counts
    private Map<ExchangeBatch, AtomicInteger> countResourcesSaved = new ConcurrentHashMap<>();
    private Map<ExchangeBatch, AtomicInteger> countResourcesDeleted = new ConcurrentHashMap<>();


    public FhirResourceFiler(UUID exchangeId, UUID serviceId, UUID systemId, TransformError transformError,
                             List<UUID> batchIdsCreated) throws Exception {
        this.exchangeId = exchangeId;
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.storageService = new FhirStorageService(serviceId, systemId);
        this.exchangeBatchRepository = DalProvider.factoryExchangeBatchDal();
        this.transformError = transformError;
        this.batchIdsCreated = batchIdsCreated;

        //base the thread pools on the connection pool max size minus a bit of room
        int maxFilingThreads = ConnectionManager.getEhrConnectionPoolMaxSize(serviceId) - 2;
        this.threadPoolIdMapper = new ThreadPool(maxFilingThreads, 25000);
        this.threadPoolFiler = new ThreadPool(maxFilingThreads, 25000);
        this.creationTime = System.currentTimeMillis();
    }


    public void saveAdminResource(CsvCurrentState parserState, Resource... resources) throws Exception {
        saveAdminResource(parserState, true, resources);
    }
    public void saveAdminResource(CsvCurrentState parserState, boolean mapIds, Resource... resources) throws Exception {
        ExchangeBatch batch = getAdminBatch();
        ResourceBuilderBase[] resourceBuilders = wrapResourcesInBuilders(resources);
        addResourceToQueue(parserState, false, mapIds, batch, false, resourceBuilders);
    }

    public void deleteAdminResource(CsvCurrentState parserState, Resource... resources) throws Exception {
        deleteAdminResource(parserState, true, resources);
    }
    public void deleteAdminResource(CsvCurrentState parserState, boolean mapIds, Resource... resources) throws Exception {
        ExchangeBatch batch = getAdminBatch();
        ResourceBuilderBase[] resourceBuilders = wrapResourcesInBuilders(resources);
        addResourceToQueue(parserState, false, mapIds, batch, true, resourceBuilders);
    }

    public void savePatientResource(CsvCurrentState parserState, Resource... resources) throws Exception {
        savePatientResource(parserState, true, resources);
    }
    public void savePatientResource(CsvCurrentState parserState, boolean mapIds, Resource... resources) throws Exception {
        ResourceBuilderBase[] resourceBuilders = wrapResourcesInBuilders(resources);
        ExchangeBatch batch = getPatientBatch(mapIds, resourceBuilders);
        addResourceToQueue(parserState, true, mapIds, batch, false, resourceBuilders);
    }

    public void deletePatientResource(CsvCurrentState parserState, Resource... resources) throws Exception {
        deletePatientResource(parserState, true, resources);
    }
    public void deletePatientResource(CsvCurrentState parserState, boolean mapIds, Resource... resources) throws Exception {
        ResourceBuilderBase[] resourceBuilders = wrapResourcesInBuilders(resources);
        ExchangeBatch batch = getPatientBatch(mapIds, resourceBuilders);
        addResourceToQueue(parserState, true, mapIds, batch, true, resourceBuilders);
    }


    private static ResourceBuilderBase[] wrapResourcesInBuilders(Resource[] resources) {
        List<ResourceBuilderBase> list = new ArrayList<>();
        for (Resource resource: resources) {
            GenericBuilder builder = new GenericBuilder(resource);
            list.add(builder);
        }
        return list.toArray(new ResourceBuilderBase[list.size()]);
    }


    public void saveAdminResource(CsvCurrentState parserState, ResourceBuilderBase... resources) throws Exception {
        saveAdminResource(parserState, true, resources);
    }
    public void saveAdminResource(CsvCurrentState parserState, boolean mapIds, ResourceBuilderBase... resources) throws Exception {
        ExchangeBatch batch = getAdminBatch();
        addResourceToQueue(parserState, false, mapIds, batch, false, resources);
    }

    public void deleteAdminResource(CsvCurrentState parserState, ResourceBuilderBase... resources) throws Exception {
        deleteAdminResource(parserState, true, resources);
    }
    public void deleteAdminResource(CsvCurrentState parserState, boolean mapIds, ResourceBuilderBase... resources) throws Exception {
        ExchangeBatch batch = getAdminBatch();
        addResourceToQueue(parserState, false, mapIds, batch, true, resources);
    }

    public void savePatientResource(CsvCurrentState parserState, ResourceBuilderBase... resources) throws Exception {
        savePatientResource(parserState, true, resources);
    }
    public void savePatientResource(CsvCurrentState parserState, boolean mapIds, ResourceBuilderBase... resources) throws Exception {
        ExchangeBatch batch = getPatientBatch(mapIds, resources);
        addResourceToQueue(parserState, true, mapIds, batch, false, resources);
    }

    public void deletePatientResource(CsvCurrentState parserState, ResourceBuilderBase... resources) throws Exception {
        deletePatientResource(parserState, true, resources);
    }
    public void deletePatientResource(CsvCurrentState parserState, boolean mapIds, ResourceBuilderBase... resources) throws Exception {
        ExchangeBatch batch = getPatientBatch(mapIds, resources);
        addResourceToQueue(parserState, true, mapIds, batch, true, resources);
    }

    private void addResourceToQueue(CsvCurrentState parserState,
                                    boolean expectingPatientResource,
                                    boolean mapIds,
                                    ExchangeBatch exchangeBatch,
                                    boolean isDelete,
                                    ResourceBuilderBase... resourceBuilders) throws Exception {

        for (ResourceBuilderBase resourceBuilder: resourceBuilders) {

            //validate we're treating the resource properly as admin / patient
            Resource resource = resourceBuilder.getResource();
            if (isPatientResource(resource) != expectingPatientResource) {
                throw new PatientResourceException(resource, expectingPatientResource);
            }

            /*String resourceType = resource.getResourceType().toString();
            resourceTypes.put(resourceType, resourceType);*/

            //increment our counters for auditing
            if (isDelete) {
                countResourcesDeleted.get(exchangeBatch).incrementAndGet();
            } else {
                countResourcesSaved.get(exchangeBatch).incrementAndGet();
            }
        }

        //if we want to map IDs then put in the ID mapping queue, otherwise go straight to the filing queue
        if (mapIds) {
            addToIdMappingQueue(parserState, isDelete, exchangeBatch, resourceBuilders);

        } else {
            for (ResourceBuilderBase resourceBuilder: resourceBuilders) {
                addToFilingQueue(parserState, isDelete, exchangeBatch, resourceBuilder, false);
            }
        }
    }

    private void addToIdMappingQueue(CsvCurrentState parserState, boolean isDelete,
                                     ExchangeBatch exchangeBatch, ResourceBuilderBase[] resourceBuilders) throws Exception {

        //we batch up ID mapping as it's more efficient to do multiple at once
        MapIdJob job = new MapIdJob(parserState, isDelete, exchangeBatch, resourceBuilders);
        nextMapIdTask.addJob(job);

        //if the task is full, then execute it
        if (nextMapIdTask.isFull()) {
            runNextMapIdTask();
        }
    }

    private void runNextMapIdTask() throws Exception {
        MapIdTask task = nextMapIdTask;
        nextMapIdTask = new MapIdTask();

        List<ThreadPoolError> errors = threadPoolIdMapper.submit(task);
        handleErrors(errors);
    }

    private void addToFilingQueue(CsvCurrentState parserState, boolean isDelete, ExchangeBatch exchangeBatch,
                                  ResourceBuilderBase resourceBuilder, boolean isDefinitelyNewResource) throws Exception {

        FileResourceTask task = new FileResourceTask(parserState, isDelete, exchangeBatch, resourceBuilder, isDefinitelyNewResource);
        List<ThreadPoolError> errors = threadPoolFiler.submit(task);
        handleErrors(errors);
    }



    private ExchangeBatch getAdminBatch() throws Exception {
        if (adminBatchId == null) {

            try {
                batchIdLock.lock();

                //make sure to check if it's still null, as another thread may have created the ID while we were waiting to batchIdLock
                if (adminBatchId == null) {
                    adminBatchId = createAndSaveExchangeBatch(null);
                }
            } finally {
                batchIdLock.unlock();
            }
        }
        return adminBatchId;
    }

    private ExchangeBatch getPatientBatch(boolean mapIds, ResourceBuilderBase... resourceBuilders) throws Exception {

        UUID edsPatientId = findEdsPatientId(mapIds, resourceBuilders);

        ExchangeBatch patientBatch = patientBatchIdMap.get(edsPatientId);
        if (patientBatch == null) {

            try {
                batchIdLock.lock();

                //make sure to check if it's still null, as another thread may have created the ID while we were waiting to batchIdLock
                patientBatch = patientBatchIdMap.get(edsPatientId);

                if (patientBatch == null) {
                    //we need to generate/find the EDS patient UUID to go in the batch
                    patientBatch = createAndSaveExchangeBatch(edsPatientId);

                    //but hash by the raw patient ID, so we can quickly look up the batch for subsequent resources
                    patientBatchIdMap.put(edsPatientId, patientBatch);
                }
            } finally {
                batchIdLock.unlock();
            }
        }
        return patientBatch;
    }

    /**
     * find or creates the EDS patient ID for the given resources, which may have EDS IDs
     * already in them or may have local IDs and need ID mapping
     */
    private UUID findEdsPatientId(boolean mapIds, ResourceBuilderBase... resourceBuilders) throws Exception {

        for (ResourceBuilderBase resourceBuilder: resourceBuilders) {

            try {

                UUID ret = null;

                //get the patient reference from the resource
                Resource resource = resourceBuilder.getResource();
                String resourcePatientId = IdHelper.getPatientId(resource);

                if (mapIds) {
                    //if we need to ID map, then the patient ID on the resource is the RAW patient ID (e.g. Emis GUID),
                    //so we need to translate that to an EDS ID

                    //check our local lookup cache first
                    ret = sourcePatientIdMap.get(resourcePatientId);
                    if (ret == null) {

                        //if not in our local lookup, then use the ID mapper layer to lookup and then add to our local cache
                        String edsPatientIdStr = IdHelper.getOrCreateEdsResourceIdString(serviceId, systemId, ResourceType.Patient, resourcePatientId);
                        ret = UUID.fromString(edsPatientIdStr);

                        //apply any merged resource mapping
                        String patientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, edsPatientIdStr);
                        Map<String, String> pastMergeReferences = ResourceMergeMapHelper.getResourceMergeMappings(serviceId);
                        String mappedPatientReference = pastMergeReferences.get(patientReference);
                        if (mappedPatientReference != null) {
                            ReferenceComponents comps = ReferenceHelper.getReferenceComponents(new Reference().setReference(mappedPatientReference));
                            String newPatientReference = comps.getId();
                            ret = UUID.fromString(newPatientReference);
                        }

                        sourcePatientIdMap.put(resourcePatientId, ret);
                    }

                } else {
                    //if we're not ID mapping, then the patient ID on the resource IS the EDS patient ID
                    ret = UUID.fromString(resourcePatientId);
                }

                return ret;

            } catch (Exception ex) {
                //if we try this on a Slot, it'll fail as it doesn't have a patient but we treat it like a patient resource, so just ignore any errors
            }
        }

        //if we get here, something is wrong since we've failed to find a patient ID
        LOG.error("No patient reference found for resources:");
        for (ResourceBuilderBase resourceBuilder: resourceBuilders) {
            Resource resource = resourceBuilder.getResource();
            LOG.error("" + FhirSerializationHelper.serializeResource(resource));
        }
        throw new TransformException("Failed to find or create EDS patient ID");
    }


    private ExchangeBatch createAndSaveExchangeBatch(UUID edsPatientId) throws Exception {

        ExchangeBatch exchangeBatch = createExchangeBatch(exchangeId, edsPatientId);
        exchangeBatchRepository.save(exchangeBatch);

        UUID batchId = exchangeBatch.getBatchId();
        batchIdsCreated.add(batchId);

        countResourcesDeleted.put(exchangeBatch, new AtomicInteger());
        countResourcesSaved.put(exchangeBatch, new AtomicInteger());

        return exchangeBatch;
    }

    public static ExchangeBatch createExchangeBatch(UUID exchangeId, UUID edsPatientId) {

        ExchangeBatch ret = new ExchangeBatch();
        ret.setBatchId(UUIDs.timeBased());
        ret.setExchangeId(exchangeId);
        ret.setInsertedAt(new Date());
        ret.setEdsPatientId(edsPatientId);

        return ret;
    }

    /**
     * called after all content has been processed. It blocks until all operations have
     * been completed in the thread pool, then returns the distinct batch IDs created
     */
    public void waitToFinish() throws Exception {

        //make sure the current job of ID mapping gets run
        runNextMapIdTask();

        //close down the ID mapper pool
        List<ThreadPoolError> errors = threadPoolIdMapper.waitAndStop();
        handleErrors(errors);

        //close down the filing pool
        errors = threadPoolFiler.waitAndStop();
        handleErrors(errors);

        //log out counts of what we processed
        logResults();
    }

    /**
     * whenever anything it added to one of the thread pools, we may get one or more errors
     * returned for previously submitted tasks. This function handles those errors.
     * Note, this is synchronised because we add to thread pools from both the main transform
     * thread and the ID mapping threads
     */
    private synchronized void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        for (ThreadPoolError error: errors) {

            Throwable cause = error.getException();

            //if the exception is one of our special types, then we
            if (cause instanceof FilingAndMappingException) {
                FilingAndMappingException filingAndMappingException = (FilingAndMappingException)cause;
                List<CsvCurrentState> parserStates = filingAndMappingException.getParserStates();
                Throwable innerCause = cause.getCause();

                if (parserStates != null
                    && !parserStates.isEmpty()) {

                    for (CsvCurrentState parserState: parserStates) {
                        logTransformRecordError(innerCause, parserState);
                    }

                    return;
                }
            }

            //if we had an error that doesn't have a CSV state, then it's not something that can be attributed
            //to a specific row in a CSV file, and so should be treated as a fatal exception

            //the cause may be an Exception or Error so we need to explicitly
            //cast to the right type to throw it without changing the method signature
            if (cause instanceof Exception) {
                throw (Exception)cause;
            } else if (cause instanceof Error) {
                throw (Error)cause;
            }
        }
    }

    /*private void saveResourceTypesUsed() {

        ResourceRepository resourceRepository = new ResourceRepository();

        Iterator<String> it = resourceTypes.keySet().iterator();
        while (it.hasNext()) {
            String resourceType = it.next();
            ResourceTypesUsed resourceTypesUsed = new ResourceTypesUsed();
            resourceTypesUsed.setServiceId(serviceId);
            resourceTypesUsed.setSystemId(systemId);
            resourceTypesUsed.setResourceType(resourceType);

            resourceRepository.save(resourceTypesUsed);
        }
    }*/


    private void logResults() throws Exception {

        int adminSaved = 0;
        int adminDeleted = 0;
        if (adminBatchId != null) {
            adminSaved += countResourcesSaved.get(adminBatchId).get();
            adminDeleted += countResourcesDeleted.get(adminBatchId).get();
        }

        int patientSaved = 0;
        int patientDeleted = 0;
        int patientCount = patientBatchIdMap.size();

        for (ExchangeBatch exchangeBatch : patientBatchIdMap.values()) {
            patientSaved += countResourcesSaved.get(exchangeBatch).get();
            patientDeleted += countResourcesDeleted.get(exchangeBatch).get();
        }

        long durationMillis = System.currentTimeMillis() - creationTime;
        long durationSeconds = durationMillis / 1000L;
        long durationMinutes = durationSeconds / 60L;

        LOG.info("Resource filing completed in " + durationMinutes + " min: admin resources [saved " + adminSaved + ", deleted " + adminDeleted + "]"
                + ", patient resources [saved " + patientSaved + ", deleted " + patientDeleted + " over " + patientCount + " patients]");

        //adding a slack alert so we proactively know when a practie has been deleted
        //note the numbers are just arbitrary, because I'm not aware of any GP practice of less than 2000 patients
        if (patientCount > 2000
                && patientDeleted > 1000000) {

            ServiceDalI serviceDal = DalProvider.factoryServiceDal();
            Service service = serviceDal.getById(serviceId);
            String msg = "" + patientDeleted + " resources deleted over "
                       + patientCount + " patients in exchange "
                       + exchangeId + " for " + service.getName() + " " + service.getId();

            SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, msg);
        }
    }

    /*private List<UUID> getAllBatchIds() {
        List<UUID> batchIds = new ArrayList<>();
        if (adminBatchId != null) {
            batchIds.add(adminBatchId);
        }
        Iterator<UUID> it = patientBatchIdMap.values().iterator();
        while (it.hasNext()) {
            UUID batchId = it.next();
            batchIds.add(batchId);
        }
        return batchIds;
    }*/

    public static boolean isPatientResource(Resource resource) {
        return isPatientResource(resource.getResourceType());
    }

    public static boolean isPatientResource(ResourceType type) {

        if (patientResourceTypes == null) {
            Set<ResourceType> set = new HashSet<>();
            set.add(ResourceType.AllergyIntolerance);
            set.add(ResourceType.Appointment);
            set.add(ResourceType.Condition);
            set.add(ResourceType.DiagnosticOrder);
            set.add(ResourceType.DiagnosticReport);
            set.add(ResourceType.Encounter);
            set.add(ResourceType.EpisodeOfCare);
            set.add(ResourceType.FamilyMemberHistory);
            set.add(ResourceType.Immunization);
            set.add(ResourceType.MedicationOrder);
            set.add(ResourceType.MedicationStatement);
            set.add(ResourceType.Observation);
            set.add(ResourceType.Order);
            set.add(ResourceType.Patient);
            set.add(ResourceType.Procedure);
            set.add(ResourceType.ProcedureRequest);
            set.add(ResourceType.ReferralRequest);
            set.add(ResourceType.RelatedPerson);
            set.add(ResourceType.Specimen);

            //although Slot isn't technically linked to a patient, it is saved at the same time as
            //Appointment resources, so should be treated as one
            set.add(ResourceType.Slot);

            patientResourceTypes = set;
        }

        return patientResourceTypes.contains(type);
    }

    public UUID getServiceId() {
        return serviceId;
    }

    public UUID getSystemId() {
        return systemId;
    }

    /**
     * called when an exception occurs when processing a record in a CSV file, which stores the error in
     * a table which can then be used to re-play the transform for just those records that were in error
     */
    public void logTransformRecordError(Throwable ex, CsvCurrentState state) {

        //if we've had more than 100 errors, don't bother logging or adding any more exceptions to the audit trail
        if (transformError.getError().size() > 50) {
            LOG.error("Error at " + state + ": " + ex.getMessage() + " (had over 50 exceptions, so not logging any more)");
            ex = null;

        } else {
            //don't log the exception here, since we've already logged it from the separate thread
            //LOG.error("Error at " + state + ": " + ex.getMessage());
            LOG.error("Error at " + state, ex);
        }

        //then add the error to our audit object
        Map<String, String> args = new HashMap<>();
        //args.put(TransformErrorUtility.ARG_EMIS_CSV_DIRECTORY, state.getFileDir());
        args.put(TransformErrorUtility.ARG_EMIS_CSV_FILE, state.getFileName());
        args.put(TransformErrorUtility.ARG_EMIS_CSV_RECORD_NUMBER, "" + state.getRecordNumber());

        TransformErrorUtility.addTransformError(transformError, ex, args);
    }

    class MapIdJob {
        private CsvCurrentState parserState = null;
        private boolean isDelete = false;
        private ExchangeBatch exchangeBatch = null;
        private ResourceBuilderBase[] resourceBuilders = null;

        MapIdJob(CsvCurrentState parserState, boolean isDelete, ExchangeBatch exchangeBatch, ResourceBuilderBase... resourceBuilders) {
            this.parserState = parserState;
            this.isDelete = isDelete;
            this.exchangeBatch = exchangeBatch;
            this.resourceBuilders = resourceBuilders;
        }

        public CsvCurrentState getParserState() {
            return parserState;
        }

        public void setParserState(CsvCurrentState parserState) {
            this.parserState = parserState;
        }

        public boolean isDelete() {
            return isDelete;
        }

        public void setDelete(boolean delete) {
            isDelete = delete;
        }

        public ExchangeBatch getExchangeBatch() {
            return exchangeBatch;
        }

        public void setExchangeBatch(ExchangeBatch exchangeBatch) {
            this.exchangeBatch = exchangeBatch;
        }

        public ResourceBuilderBase[] getResourceBuilders() {
            return resourceBuilders;
        }

        public void setResourceBuilders(ResourceBuilderBase[] resourceBuilders) {
            this.resourceBuilders = resourceBuilders;
        }
    }

    class FilingAndMappingException extends Exception {
        private List<CsvCurrentState> parserStates = null;

        public FilingAndMappingException(String message, CsvCurrentState parserState, Exception cause) {
            super(message, cause);

            if (parserState != null) {
                parserStates = new ArrayList<>();
                this.parserStates.add(parserState);
            }
        }

        public FilingAndMappingException(String message, List<CsvCurrentState> parserStates, Exception cause) {
            super(message, cause);

            this.parserStates = parserStates;
        }

        public List<CsvCurrentState> getParserStates() {
            return parserStates;
        }
    }


    class MapIdTask implements Callable {

        private List<MapIdJob> jobs = new ArrayList<>();

        public MapIdTask() { }

        public void addJob(MapIdJob job) {
            jobs.add(job);
        }

        public boolean isFull() {
            return jobs.size() >= 10;
        }

        @Override
        public Object call() throws Exception {

            //collate all the resources from the jobs into a single list, with a map for reverse lookups
            List<Resource> resources = new ArrayList<>();
            Map<Resource, MapIdJob> hmJobsByResource = new HashMap<>();

            List<ResourceBuilderBase> resourceBuilders = new ArrayList<>();

            for (MapIdJob job: jobs) {
                ResourceBuilderBase[] resourceBuildersArr = job.getResourceBuilders();
                for (ResourceBuilderBase resourceBuilder: resourceBuildersArr) {
                    Resource resource = resourceBuilder.getResource();
                    resources.add(resource);
                    hmJobsByResource.put(resource, job);
                    resourceBuilders.add(resourceBuilder);
                }
            }

            //do the ID mapping
            Set<Resource> definitelyNewResources = null;
            try {
                definitelyNewResources = IdHelper.mapIds(serviceId, systemId, resources);

            } catch (Exception ex) {

                String err = "Exception mapping resources: ";
                List<CsvCurrentState> parserStates = new ArrayList<>();
                for (Resource resource: resources) {
                    err += resource.getResourceType() + "/" + resource.getId() + " ";

                    if (parserStates != null) {
                        MapIdJob job = hmJobsByResource.get(resource);
                        CsvCurrentState parserState = job.getParserState();
                        if (parserState == null) {
                            //if one of our jobs has a null parser state, then it's not a CSV-record level error
                            //so null the list so it gets treated as a fatal error
                            parserStates = null;
                        } else {
                            parserStates.add(parserState);
                        }
                    }
                }

                LOG.error(err, ex);
                throw new FilingAndMappingException(err, parserStates, ex);
            }

            //then bump onto the filing queue
            for (ResourceBuilderBase resourceBuilder: resourceBuilders) {
                Resource resource = resourceBuilder.getResource();
                boolean isDefinitelyNewResource = definitelyNewResources.contains(resource);
                MapIdJob job = hmJobsByResource.get(resource);
                CsvCurrentState parserState = job.getParserState();
                boolean isDelete = job.isDelete();
                ExchangeBatch exchangeBatch = job.exchangeBatch;

                addToFilingQueue(parserState, isDelete, exchangeBatch, resourceBuilder, isDefinitelyNewResource);
            }

            return null;
        }
    }

    class FileResourceTask implements Callable {

        private CsvCurrentState parserState = null;
        private boolean isDelete = false;
        private ExchangeBatch exchangeBatch = null;
        private ResourceBuilderBase resourceBuilder = null;
        private boolean isDefinitelyNewResource = false;

        public FileResourceTask(CsvCurrentState parserState, boolean isDelete, ExchangeBatch exchangeBatch,
                                ResourceBuilderBase resourceBuilder, boolean isDefinitelyNewResource) {
            this.parserState = parserState;
            this.isDelete = isDelete;
            this.exchangeBatch = exchangeBatch;
            this.resourceBuilder = resourceBuilder;
            this.isDefinitelyNewResource = isDefinitelyNewResource;
        }

        @Override
        public Object call() throws Exception {

            Resource resource = resourceBuilder.getResource();

            try {
                //apply any existing merge mappings to the resource
                Map<String, String> pastMergeReferences = ResourceMergeMapHelper.getResourceMergeMappings(serviceId);
                IdHelper.applyExternalReferenceMappings(resource, pastMergeReferences, false);

                //save or delete the resource
                ResourceWrapper resourceWrapper = null;
                UUID batchUuid = exchangeBatch.getBatchId();
                if (isDelete) {
                    resourceWrapper = storageService.exchangeBatchDelete(exchangeId, batchUuid, resource);
                } else {
                    resourceWrapper = storageService.exchangeBatchUpdate(exchangeId, batchUuid, resource, isDefinitelyNewResource);
                }

                //store our audit trail if we actually saved the resource
                if (resourceWrapper != null) {
                    ResourceFieldMappingAudit audit = resourceBuilder.getAuditWrapper();
                    SourceFileMappingDalI dal = DalProvider.factorySourceFileMappingDal();
                    dal.saveResourceMappings(serviceId, resourceWrapper, audit);
                }

            } catch (Exception ex) {
                LOG.error("", ex);
                throw new FilingAndMappingException("Exception filing " + resource.getResourceType() + " " + resource.getId(), parserState, ex);
            }

            return null;
        }
    }

    /*class MapAndSaveResourceTask implements Callable {

        private CsvCurrentState parserState = null;
        private boolean isDelete = false;
        private boolean mapIds = false;
        private ExchangeBatch exchangeBatch = null;
        private boolean patientResources = false;
        private Resource[] resources = null;

        public MapAndSaveResourceTask(CsvCurrentState parserState, boolean isDelete, boolean mapIds,
                                      ExchangeBatch exchangeBatch, boolean patientResources, Resource... resources) {
            this.parserState = parserState;
            this.isDelete = isDelete;
            this.mapIds = mapIds;
            this.exchangeBatch = exchangeBatch;
            this.patientResources = patientResources;
            this.resources = resources;
        }

        @Override
        public Object call() throws Exception {

            for (Resource resource: resources) {

                try {
                    boolean isDefinitelyNewResource = false;
                    if (mapIds) {
                        isDefinitelyNewResource = IdHelper.mapIds(serviceId, systemId, resource);
                    }

                    UUID batchUuid = exchangeBatch.getBatchId();
                    if (isDelete) {
                        storageService.exchangeBatchDelete(exchangeId, batchUuid, resource);
                    } else {
                        storageService.exchangeBatchUpdate(exchangeId, batchUuid, resource, isDefinitelyNewResource);
                    }

                } catch (Exception ex) {
                    LOG.error("", ex);
                    throw new TransformException("Exception mapping or storing " + resource.getResourceType() + " " + resource.getId(), ex);
                }
            }

            return null;
        }

        public CsvCurrentState getParserState() {
            return parserState;
        }
    }*/




}
