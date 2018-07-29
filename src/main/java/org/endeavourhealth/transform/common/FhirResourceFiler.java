package org.endeavourhealth.transform.common;

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
import org.endeavourhealth.transform.common.resourceBuilders.ResourceBuilderBase;
import org.endeavourhealth.transform.common.resourceValidators.ResourceValidatorBase;
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

public class FhirResourceFiler implements FhirResourceFilerI, HasServiceSystemAndExchangeIdI {

    private static final Logger LOG = LoggerFactory.getLogger(FhirResourceFiler.class);

    private static Set<ResourceType> patientResourceTypes = null;
    private static Map<Class, ResourceValidatorBase> resourceValidators = new ConcurrentHashMap<>();

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
    private ReentrantLock mapIdTaskLock = new ReentrantLock();
    private MapIdTask nextMapIdTask = new MapIdTask();
    private ReentrantLock resourceTaskLock = new ReentrantLock();
    private FileResourceTask nextSaveResourceTask = new FileResourceTask(false);
    private FileResourceTask nextDeleteResourceTask = new FileResourceTask(true);
    private ThreadPool threadPoolIdMapper = null;
    private ThreadPool threadPoolFiler = null;

    //counts
    private Map<ExchangeBatch, AtomicInteger> countResourcesSaved = new ConcurrentHashMap<>();
    private Map<ExchangeBatch, AtomicInteger> countResourcesDeleted = new ConcurrentHashMap<>();

    //error handling
    private Throwable lastExceptionRecorded;

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
        this.threadPoolIdMapper = new ThreadPool(maxFilingThreads, 1000);
        this.threadPoolFiler = new ThreadPool(maxFilingThreads, 1000);
        this.creationTime = System.currentTimeMillis();
    }


    public void saveAdminResource(CsvCurrentState parserState, ResourceBuilderBase... resources) throws Exception {
        saveAdminResource(parserState, true, resources);
    }
    public void saveAdminResource(CsvCurrentState parserState, boolean mapIds, ResourceBuilderBase... resources) throws Exception {
        validateResources(serviceId, mapIds, false, resources);
        ExchangeBatch batch = getAdminBatch();
        addResourceToQueue(parserState, false, mapIds, batch, false, resources);
    }

    public void deleteAdminResource(CsvCurrentState parserState, ResourceBuilderBase... resources) throws Exception {
        deleteAdminResource(parserState, true, resources);
    }
    public void deleteAdminResource(CsvCurrentState parserState, boolean mapIds, ResourceBuilderBase... resources) throws Exception {
        validateResources(serviceId, mapIds, true, resources);
        ExchangeBatch batch = getAdminBatch();
        addResourceToQueue(parserState, false, mapIds, batch, true, resources);
    }

    public void savePatientResource(CsvCurrentState parserState, ResourceBuilderBase... resources) throws Exception {
        savePatientResource(parserState, true, resources);
    }
    public void savePatientResource(CsvCurrentState parserState, boolean mapIds, ResourceBuilderBase... resources) throws Exception {
        validateResources(serviceId, mapIds, false, resources);
        ExchangeBatch batch = getPatientBatch(mapIds, resources);
        addResourceToQueue(parserState, true, mapIds, batch, false, resources);
    }

    public void deletePatientResource(CsvCurrentState parserState, ResourceBuilderBase... resources) throws Exception {
        deletePatientResource(parserState, true, resources);
    }
    public void deletePatientResource(CsvCurrentState parserState, boolean mapIds, ResourceBuilderBase... resources) throws Exception {
        validateResources(serviceId, mapIds, true, resources);
        ExchangeBatch batch = getPatientBatch(mapIds, resources);
        addResourceToQueue(parserState, true, mapIds, batch, true, resources);
    }

    private void addResourceToQueue(CsvCurrentState parserState,
                                    boolean expectingPatientResource,
                                    boolean mapIds,
                                    ExchangeBatch exchangeBatch,
                                    boolean isDelete,
                                    ResourceBuilderBase... resourceBuilders) throws Exception {

        if (TransformConfig.instance().isDisableSavingResources()) {
            LOG.info("NOT SAVING RESOURCES AS DISABLED IN TRANSFORM CONFIG");
            return;
        }

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
            addToIdMappingQueue(parserState, isDelete, exchangeBatch.getBatchId(), resourceBuilders);

        } else {
            addToFilingQueue(parserState, isDelete, exchangeBatch.getBatchId(), resourceBuilders);
        }
    }

    private void addToIdMappingQueue(CsvCurrentState parserState, boolean isDelete,
                                     UUID batchId, ResourceBuilderBase[] resourceBuilders) throws Exception {

        //lock since this may be called from multiple threads and array lists aren't thread safe for adding
        try {
            mapIdTaskLock.lock();

            for (ResourceBuilderBase builder : resourceBuilders) {
                ResourceJob job = new ResourceJob(parserState, isDelete, batchId, builder);
                nextMapIdTask.addJob(job);
            }

            //if the task is full, then execute it
            if (nextMapIdTask.isFull()) {
                runNextMapIdTask();
            }

        } finally {
            mapIdTaskLock.unlock();
        }
    }

    private void runNextMapIdTask() throws Exception {
        try {
            mapIdTaskLock.lock();

            MapIdTask task = nextMapIdTask;
            nextMapIdTask = new MapIdTask();

            if (!task.isEmpty()) {
                List<ThreadPoolError> errors = threadPoolIdMapper.submit(task);
                handleErrors(errors);
            }
        } finally {
            mapIdTaskLock.unlock();
        }
    }

    private void addToFilingQueue(CsvCurrentState parserState, boolean isDelete,
                                  UUID batchId, ResourceBuilderBase[] resourceBuilders) throws Exception {

        for (ResourceBuilderBase builder: resourceBuilders) {
            ResourceJob job = new ResourceJob(parserState, isDelete, batchId, builder);
            addToFilingQueue(job);
        }
    }

    private void addToFilingQueue(ResourceJob job) throws Exception {
        //lock since this may be called from multiple threads and array lists aren't thread safe for adding
        try {
            resourceTaskLock.lock();

            if (job.isDelete()) {
                nextDeleteResourceTask.addJob(job);
            } else {
                nextSaveResourceTask.addJob(job);
            }

            if (job.isDelete()) {
                if (nextDeleteResourceTask.isFull()) {
                    runNextDeleteResourceTask();
                }
            } else {
                if (nextSaveResourceTask.isFull()) {
                    runNextSaveResourceTask();
                }
            }

        } finally {
            resourceTaskLock.unlock();
        }
    }

    private void runNextSaveResourceTask() throws Exception {
        try {
            resourceTaskLock.lock();

            FileResourceTask task = nextSaveResourceTask;
            nextSaveResourceTask = new FileResourceTask(false);

            if (!task.isEmpty()) {
                List<ThreadPoolError> errors = threadPoolFiler.submit(task);
                handleErrors(errors);
            }

        } finally {
            resourceTaskLock.unlock();
        }

    }

    private void runNextDeleteResourceTask() throws Exception {
        try {
            resourceTaskLock.lock();

            FileResourceTask task = nextDeleteResourceTask;
            nextDeleteResourceTask = new FileResourceTask(true);

            if (!task.isEmpty()) {
                List<ThreadPoolError> errors = threadPoolFiler.submit(task);
                handleErrors(errors);
            }

        } finally {
            resourceTaskLock.unlock();
        }
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
                        String edsPatientIdStr = IdHelper.getOrCreateEdsResourceIdString(serviceId, ResourceType.Patient, resourcePatientId);
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
        ret.setBatchId(UUID.randomUUID());
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

        //now all mapping is complete, start the last filing tasks
        runNextSaveResourceTask();
        runNextDeleteResourceTask();

        //close down the filing pool
        errors = threadPoolFiler.waitAndStop();
        handleErrors(errors);

        //log out counts of what we processed
        logResults();
    }

    /**
     * called to block until everything currently queued is saved, but DOES NOT
     * prevent anything further being added to the queues
     */
    public void waitUntilEverythingIsSaved() throws Exception {

        //make sure the current job of ID mapping gets run
        runNextMapIdTask();

        List<ThreadPoolError> errors = threadPoolIdMapper.waitUntilEmpty();
        handleErrors(errors);

        runNextSaveResourceTask();
        runNextDeleteResourceTask();

        errors = threadPoolFiler.waitUntilEmpty();
        handleErrors(errors);
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
            set.add(ResourceType.Flag);
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

    @Override
    public UUID getServiceId() {
        return serviceId;
    }

    @Override
    public UUID getSystemId() {
        return systemId;
    }

    @Override
    public UUID getExchangeId() {
        return exchangeId;
    }

    /**
     * called when an exception occurs when processing a record in a CSV file, which stores the error in
     * a table which can then be used to re-play the transform for just those records that were in error
     */
    public void logTransformRecordError(Throwable ex, CsvCurrentState state) throws Exception {

        this.lastExceptionRecorded = ex;

        //if we've had more than X errors, abort the transform
        int abortLimit = TransformConfig.instance().getMaxTransformErrorsBeforeAbort();
        if (transformError.getError().size() >= abortLimit) {
            throw new TransformException("Had " + abortLimit + " errors so aborting the transform", ex);
        }

        //don't log the exception here, since we've already logged it from the separate thread
        //LOG.error("Error at " + state + ": " + ex.getMessage());
        LOG.error("Error at " + state, ex);

        //then add the error to our audit object
        Map<String, String> args = new HashMap<>();
        //args.put(TransformErrorUtility.ARG_EMIS_CSV_DIRECTORY, state.getFileDir());
        args.put(TransformErrorUtility.ARG_EMIS_CSV_FILE, state.getFileName());
        args.put(TransformErrorUtility.ARG_EMIS_CSV_RECORD_NUMBER, "" + state.getRecordNumber());

        TransformErrorUtility.addTransformError(transformError, ex, args);
    }

    /**
     * called to check if we've logged any errors, and throws an exception if so, to abortt the transform
     */
    public void failIfAnyErrors() throws Exception {

        if (this.lastExceptionRecorded != null) {
            throw new TransformException("Had at least one errors during the last file so aborting the transform", lastExceptionRecorded);
        }
    }

    class ResourceJob {
        private CsvCurrentState parserState = null;
        private boolean isDelete = false;
        private UUID batchId = null;
        private ResourceBuilderBase resourceBuilder = null;
        private boolean isDefinitelyNewResource = false;

        ResourceJob(CsvCurrentState parserState, boolean isDelete, UUID batchId, ResourceBuilderBase resourceBuilder) {
            this.parserState = parserState;
            this.isDelete = isDelete;
            this.batchId = batchId;
            this.resourceBuilder = resourceBuilder;
        }

        public CsvCurrentState getParserState() {
            return parserState;
        }

        public boolean isDelete() {
            return isDelete;
        }

        public UUID getBatchId() {
            return batchId;
        }

        public ResourceBuilderBase getResourceBuilder() {
            return resourceBuilder;
        }


        public boolean isDefinitelyNewResource() {
            return isDefinitelyNewResource;
        }

        public void setDefinitelyNewResource(boolean definitelyNewResource) {
            isDefinitelyNewResource = definitelyNewResource;
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

        private List<ResourceJob> jobs = new ArrayList<>();

        public MapIdTask() { }

        public void addJob(ResourceJob job) {
            jobs.add(job);
        }

        public boolean isFull() {
            return jobs.size() >= 10;
        }

        public boolean isEmpty() {
            return jobs.isEmpty();
        }

        @Override
        public Object call() throws Exception {

            //collate all the resources from the jobs into a single list, with a map for reverse lookups
            List<Resource> resources = new ArrayList<>();
            Map<Resource, ResourceJob> hmJobsByResource = new HashMap<>();

            List<ResourceBuilderBase> resourceBuilders = new ArrayList<>();

            for (ResourceJob job: jobs) {
                ResourceBuilderBase resourceBuilder = job.getResourceBuilder();
                Resource resource = resourceBuilder.getResource();
                resources.add(resource);
                hmJobsByResource.put(resource, job);
                resourceBuilders.add(resourceBuilder);
            }

            //do the ID mapping
            Set<Resource> definitelyNewResources = null;
            try {
                definitelyNewResources = IdHelper.mapIds(serviceId, systemId, resources);

            } catch (Exception ex) {

                String err = "Exception mapping resources: ";
                List<CsvCurrentState> parserStates = new ArrayList<>();
                StringBuilder sb = new StringBuilder();
                for (Resource resource: resources) {
                    err += resource.getResourceType() + "/" + resource.getId() + " ";
                    if (parserStates != null) {
                        ResourceJob job = hmJobsByResource.get(resource);
                        CsvCurrentState parserState = job.getParserState();
                        if (parserState == null) {
                            //if one of our jobs has a null parser state, then it's not a CSV-record level error
                            //so null the list so it gets treated as a fatal error
                            parserStates = null;
                        } else {
                            sb.append(parserState.toString());
                            parserStates.add(parserState);
                        }
                    }
                }
                LOG.error("Parser states:" + sb.toString());
                LOG.error(err, ex);
                throw new FilingAndMappingException(err, parserStates, ex);
            }

            //then bump onto the filing queue
            for (ResourceBuilderBase resourceBuilder: resourceBuilders) {
                Resource resource = resourceBuilder.getResource();
                boolean isDefinitelyNewResource = definitelyNewResources.contains(resource);

                ResourceJob job = hmJobsByResource.get(resource);
                job.setDefinitelyNewResource(isDefinitelyNewResource);

                addToFilingQueue(job);
            }

            //seem to be retaining a lot of these in memory, so de-reference the jobs to help the GC
            this.jobs = null;

            return null;
        }
    }


    private static void validateResources(UUID serviceId, boolean mapIds, boolean deleting, ResourceBuilderBase... resourceBuilders) throws Exception {

        if (!TransformConfig.instance().isValidateResourcesOnSaving()) {
            return;
        }

        for (ResourceBuilderBase resourceBuilder : resourceBuilders) {
            Resource resource = resourceBuilder.getResource();
            Class resourceCls = resource.getClass();

            ResourceValidatorBase validator = resourceValidators.get(resourceCls);
            if (validator == null) {

                String clsName = "org.endeavourhealth.transform.common.resourceValidators.ResourceValidator" + resource.getClass().getSimpleName();
                try {
                    Class cls = Class.forName(clsName);
                    validator = (ResourceValidatorBase)cls.newInstance();
                    resourceValidators.put(resourceCls, validator);

                } catch (Exception ex) {
                    //if no validator, then it doesn't matter for now
                    throw new TransformException("Exception creating ResourceValidator for " + clsName, ex);
                }
            }

            List<String> validationErrors = new ArrayList<>();
            if (deleting) {
                validator.validateResourceDelete(resource, serviceId, mapIds, validationErrors);

            } else {
                validator.validateResourceSave(resource, serviceId, mapIds, validationErrors);
            }

            if (!validationErrors.isEmpty()) {
                String msg = "Validation errors saving " + resource.getResourceType() + " " + resource.getId();
                msg += "\n";
                msg += String.join("\n", validationErrors);
                throw new TransformException(msg);
            }
        }
    }

    class FileResourceTask implements Callable {

        private boolean isDelete;
        private List<ResourceJob> jobs;

        public FileResourceTask(boolean isDelete) {
            this.isDelete = isDelete;
            this.jobs = new ArrayList<>();
        }

        public boolean isFull() {
            return jobs.size() >= TransformConfig.instance().getResourceSaveBatchSize();
        }

        public boolean isEmpty() {
            return jobs.isEmpty();
        }

        public void addJob(ResourceJob job) {
            jobs.add(job);
        }

        @Override
        public Object call() throws Exception {

            try {
                Map<Resource, UUID> hmResourcesAndBatches = new HashMap<>();
                Set<Resource> definitelyNewResources = new HashSet<>();
                Map<String, ResourceFieldMappingAudit> hmAuditsByResourceId = new HashMap<>();

                for (ResourceJob job: jobs) {
                    /*if (job == null) {
                        LOG.info("JOB IS NULL");
                        for (int i=0; i<jobs.size(); i++) {
                            LOG.info("at " + i + " = " + jobs.get(i));
                        }
                        LOG.error("NULL JOB FOUND", new Exception());
                    }*/
                    UUID batchId = job.getBatchId();
                    ResourceBuilderBase builder = job.getResourceBuilder();
                    Resource resource = builder.getResource();

                    //apply any existing merge mappings to the resource - this is done here because not all resources
                    //go through the ID mapping process, but this needs doing every time
                    try {
                        Map<String, String> pastMergeReferences = ResourceMergeMapHelper.getResourceMergeMappings(serviceId);
                        IdHelper.applyExternalReferenceMappings(resource, pastMergeReferences, false);
                    } catch (Exception ex) {
                        LOG.error("", ex);
                        throw new FilingAndMappingException("Exception applying external reference mappings to " + resource.getResourceType() + " " + resource.getId(), job.getParserState(), ex);
                    }

                    hmResourcesAndBatches.put(resource, batchId);

                    ResourceFieldMappingAudit audit = builder.getAuditWrapper();
                    String resourceId = resource.getId();
                    hmAuditsByResourceId.put(resourceId, audit);

                    if (job.isDefinitelyNewResource()) {
                        definitelyNewResources.add(resource);
                    }
                }


                //save or delete the resource
                List<ResourceWrapper> wrappersUpdated = null;
                if (isDelete) {
                    wrappersUpdated = storageService.deleteResources(exchangeId, hmResourcesAndBatches, definitelyNewResources);
                } else {
                    wrappersUpdated = storageService.saveResources(exchangeId, hmResourcesAndBatches, definitelyNewResources);
                }

                //store our audit trail if we actually saved the resource
                SourceFileMappingDalI dal = DalProvider.factorySourceFileMappingDal();
                Map<ResourceWrapper, ResourceFieldMappingAudit> hmAuditsToSave = new HashMap<>();
                for (ResourceWrapper wrapper: wrappersUpdated) {
                    ResourceFieldMappingAudit audit = hmAuditsByResourceId.get(wrapper.getResourceId().toString());
                    hmAuditsToSave.put(wrapper, audit);
                }
                dal.saveResourceMappings(hmAuditsToSave);

            } catch (Exception ex) {

                String err = "Exception filing resources: ";
                List<CsvCurrentState> parserStates = new ArrayList<>();
                StringBuilder sb = new StringBuilder();
                for (ResourceJob job: jobs) {
                    ResourceBuilderBase builder = job.getResourceBuilder();
                    Resource resource = builder.getResource();
                    err += resource.getResourceType() + "/" + resource.getId() + " ";

                    CsvCurrentState parserState = job.getParserState();
                    if (parserState == null) {
                        //if one of our jobs has a null parser state, then it's not a CSV-record level error
                        //so null the list so it gets treated as a fatal error
                        parserStates = null;
                    } else {
                        sb.append(parserState.toString());
                        parserStates.add(parserState);
                    }
                }

                LOG.error("Parser states:" + sb.toString());
                LOG.error(err, ex);
                throw new FilingAndMappingException(err, parserStates, ex);
            }

            return null;
        }
    }

}
