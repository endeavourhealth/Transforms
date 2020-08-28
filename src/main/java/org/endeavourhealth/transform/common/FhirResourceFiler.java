package org.endeavourhealth.transform.common;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.ExchangeDalI;
import org.endeavourhealth.core.database.dal.audit.models.Exchange;
import org.endeavourhealth.core.database.dal.audit.models.ExchangeBatch;
import org.endeavourhealth.core.database.dal.audit.models.HeaderKeys;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
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
    private final TransformError transformError;
    private final List<UUID> batchIdsCreated;

    //batch IDs
    private ReentrantLock batchIdLock = new ReentrantLock();
    private ExchangeBatch currentAdminBatch = null;
    private List<ExchangeBatch> allAdminBatches = new ArrayList<>();
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
    private Map<UUID, AtomicInteger> countResourcesTrySaved = new ConcurrentHashMap<>();
    private Map<UUID, AtomicInteger> countResourcesTryDeleted = new ConcurrentHashMap<>();
    private Map<UUID, AtomicInteger> countResourcesActuallySaved = new ConcurrentHashMap<>();
    private Map<UUID, AtomicInteger> countResourcesActuallyDeleted = new ConcurrentHashMap<>();

    //error handling
    private Throwable lastExceptionRecorded;

    //caches
    private Date cachedDataDate = null;
    private Map<UUID, Boolean> deletedPatientStateCache = new ConcurrentHashMap<>();

    public FhirResourceFiler(UUID exchangeId, UUID serviceId, UUID systemId, TransformError transformError,
                             List<UUID> batchIdsCreated) throws Exception {
        this.exchangeId = exchangeId;
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.storageService = new FhirStorageService(serviceId, systemId);
        this.transformError = transformError;
        this.batchIdsCreated = batchIdsCreated;

        //base the thread pools on the connection pool max size minus a bit of room
        int maxFilingThreads = ConnectionManager.getEhrConnectionPoolMaxSize(serviceId) - 2;
        this.threadPoolIdMapper = new ThreadPool(maxFilingThreads, 500, "FhirFilerIdMapper");
        this.threadPoolFiler = new ThreadPool(maxFilingThreads, 500, "FhirFilerSaver");
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

        for (int i=0; i<resourceBuilders.length; i++) {
            ResourceBuilderBase resourceBuilder = resourceBuilders[i];

            //validate we're treating the resource properly as admin / patient
            Resource resource = resourceBuilder.getResource();
            boolean isPatientRelatedResource = isPatientResource(resource);
            if (isPatientRelatedResource != expectingPatientResource) {
                throw new PatientResourceException(resource, expectingPatientResource);
            }

            //if we're saving or deleting a patient resource, cache it's state
            if (resource.getResourceType() == ResourceType.Patient) {
                cachePatientDeleteState(exchangeBatch.getEdsPatientId(), isDelete);

            } else if (isPatientRelatedResource) {
                //if we're saving a patient-related resource, then validate it's for a non-deleted patient and skip if not
                if (!validatePatientDeleteState(exchangeBatch.getEdsPatientId(), isDelete, resourceBuilder)) {
                    resourceBuilders[i] = null; //set to null so it's skipped
                    continue;
                }
            }

            //increment our counters for auditing
            if (isDelete) {
                countResourcesTryDeleted.get(exchangeBatch.getBatchId()).incrementAndGet();
            } else {
                countResourcesTrySaved.get(exchangeBatch.getBatchId()).incrementAndGet();
            }
        }

        //if we want to map IDs then put in the ID mapping queue, otherwise go straight to the filing queue
        if (mapIds) {
            addToIdMappingQueue(parserState, isDelete, exchangeBatch, resourceBuilders);

        } else {
            addToFilingQueue(parserState, isDelete, exchangeBatch, resourceBuilders);
        }
    }



    private void addToIdMappingQueue(CsvCurrentState parserState, boolean isDelete,
                                     ExchangeBatch exchangeBatch, ResourceBuilderBase[] resourceBuilders) throws Exception {

        //lock since this may be called from multiple threads and array lists aren't thread safe for adding
        try {
            mapIdTaskLock.lock();

            for (ResourceBuilderBase builder : resourceBuilders) {
                if (builder != null) {
                    ResourceJob job = new ResourceJob(parserState, isDelete, exchangeBatch, builder);
                    nextMapIdTask.addJob(job);
                }
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
                                  ExchangeBatch exchangeBatch, ResourceBuilderBase[] resourceBuilders) throws Exception {

        for (ResourceBuilderBase builder : resourceBuilders) {
            if (builder != null) {
                ResourceJob job = new ResourceJob(parserState, isDelete, exchangeBatch, builder);
                //LOG.trace("Adding filing job for " + builder.getResource().getResourceType() + " " + builder.getResource().getId() + " isDelete = " + isDelete);
                addToFilingQueue(job);
            }
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

        //LOG.trace("Running next save resource task");
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
        if (currentAdminBatch == null) {

            try {
                batchIdLock.lock();

                //make sure to check if it's still null, as another thread may have created the ID while we were waiting to batchIdLock
                if (currentAdminBatch == null) {
                    currentAdminBatch = createAndSaveExchangeBatch(null);
                    allAdminBatches.add(currentAdminBatch);
                }

            } finally {
                batchIdLock.unlock();
            }

        } else {
            //to prevent downstream queue readers having memory problems, limit the size of each admin batch
            //so generate a new admin batch ID when we hit the configured limit
            AtomicInteger adminResourceSaved = countResourcesActuallySaved.get(currentAdminBatch.getBatchId());
            int maxSize = TransformConfig.instance().getAdminBatchMaxSize();
            if (adminResourceSaved != null //we're not locked, so this may be null if the batch has just been created
                && adminResourceSaved.get() >= maxSize) {

                try {
                    batchIdLock.lock();

                    //now we're locked, check again, to prevent two threads doing this at the same time
                    adminResourceSaved = countResourcesActuallySaved.get(currentAdminBatch.getBatchId());
                    if (adminResourceSaved.get() >= maxSize) {
                        LOG.warn("Admin batch now over " + maxSize + " so creating new admin batch");
                        currentAdminBatch = createAndSaveExchangeBatch(null);
                        allAdminBatches.add(currentAdminBatch);
                    }

                } finally {
                    batchIdLock.unlock();
                }

            }
        }

        return currentAdminBatch;
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

        for (ResourceBuilderBase resourceBuilder : resourceBuilders) {

            try {

                UUID ret = null;

                //get the patient reference from the resource
                Resource resource = resourceBuilder.getResource();
                String resourcePatientId = IdHelper.getPatientId(resource);

                if (mapIds) {
                    //if we need to ID map, then the patient ID on the resource is the RAW patient ID (e.g. Emis GUID),
                    //so we need to translate that to an EDS ID
                    ret = mapLocalIdToUuid(resourcePatientId);

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
        for (ResourceBuilderBase resourceBuilder : resourceBuilders) {
            Resource resource = resourceBuilder.getResource();
            LOG.error("" + FhirSerializationHelper.serializeResource(resource));
        }
        throw new TransformException("Failed to find or create EDS patient ID");
    }

    private UUID mapLocalIdToUuid(String localId) throws Exception {
        //check our local lookup cache first
        UUID ret = sourcePatientIdMap.get(localId);
        if (ret == null) {

            //if not in our local lookup, then use the ID mapper layer to lookup and then add to our local cache
            String edsPatientIdStr = IdHelper.getOrCreateEdsResourceIdString(serviceId, ResourceType.Patient, localId);
            ret = UUID.fromString(edsPatientIdStr);
            sourcePatientIdMap.put(localId, ret);
        }
        return ret;
    }


    private ExchangeBatch createAndSaveExchangeBatch(UUID edsPatientId) throws Exception {

        ExchangeBatch exchangeBatch = createExchangeBatch(exchangeId, edsPatientId);

        //done from inside the fhirStorageService now, when saving resources
        //exchangeBatchRepository.save(exchangeBatch);

        UUID batchId = exchangeBatch.getBatchId();

        //done at the end of the transform now
        //batchIdsCreated.add(batchId);

        countResourcesTryDeleted.put(batchId, new AtomicInteger());
        countResourcesTrySaved.put(batchId, new AtomicInteger());
        countResourcesActuallyDeleted.put(batchId, new AtomicInteger());
        countResourcesActuallySaved.put(batchId, new AtomicInteger());

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

        //work out which exchange batches have been used and delete ones that didn't result in any changes
        for (ExchangeBatch exchangeBatch: allAdminBatches) {
            checkIfBatchUsed(exchangeBatch);
        }
        for (ExchangeBatch exchangeBatch : patientBatchIdMap.values()) {
            checkIfBatchUsed(exchangeBatch);
        }

        //log out counts of what we processed
        logResults();
    }

    private void checkIfBatchUsed(ExchangeBatch exchangeBatch) throws Exception {

        UUID batchId = exchangeBatch.getBatchId();
        int actuallySaved = countResourcesActuallySaved.get(batchId).get();
        int actuallyDeleted = countResourcesActuallyDeleted.get(batchId).get();

        if (actuallySaved > 0 || actuallyDeleted > 0) {
            //if we did use the batch, then add to the list so the calling pipeline knows
            //to send on to the protocol queue
            batchIdsCreated.add(batchId);
        }
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

        for (ThreadPoolError error : errors) {

            Throwable cause = error.getException();

            //if the exception is one of our special types, then we
            if (cause instanceof FilingAndMappingException) {
                FilingAndMappingException filingAndMappingException = (FilingAndMappingException) cause;
                List<CsvCurrentState> parserStates = filingAndMappingException.getParserStates();
                Throwable innerCause = cause.getCause();

                if (parserStates != null
                        && !parserStates.isEmpty()) {

                    for (CsvCurrentState parserState : parserStates) {
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
                throw (Exception) cause;
            } else if (cause instanceof Error) {
                throw (Error) cause;
            }
        }
    }


    private void logResults() throws Exception {

        //no point doing the below if we've turned off info logging
        if (!LOG.isInfoEnabled()) {
            return;
        }

        int adminTrySaved = 0;
        int adminTryDeleted = 0;
        int adminActuallySaved = 0;
        int adminActuallyDeleted = 0;

        for (ExchangeBatch exchangeBatch: allAdminBatches) {
            UUID batchId = exchangeBatch.getBatchId();
            adminTrySaved += countResourcesTrySaved.get(batchId).get();
            adminTryDeleted += countResourcesTryDeleted.get(batchId).get();
            adminActuallySaved += countResourcesActuallySaved.get(batchId).get();
            adminActuallyDeleted += countResourcesActuallyDeleted.get(batchId).get();
        }

        int patientTrySaved = 0;
        int patientTryDeleted = 0;
        int patientActuallySaved = 0;
        int patientActuallyDeleted = 0;
        int patientCount = patientBatchIdMap.size();

        for (ExchangeBatch exchangeBatch : patientBatchIdMap.values()) {
            UUID batchId = exchangeBatch.getBatchId();
            patientTrySaved += countResourcesTrySaved.get(batchId).get();
            patientTryDeleted += countResourcesTryDeleted.get(batchId).get();
            patientActuallySaved += countResourcesActuallySaved.get(batchId).get();
            patientActuallyDeleted += countResourcesActuallyDeleted.get(batchId).get();
        }

        long durationMillis = System.currentTimeMillis() - creationTime;
        long durationSeconds = durationMillis / 1000L;
        long durationMinutes = durationSeconds / 60L;

        LOG.info("Resource filing completed in " + durationMinutes + " min:"
                + " admin resources ["
                + " saved " + adminActuallySaved + "/" + adminTrySaved + ", deleted " + adminActuallyDeleted + "/" + adminTryDeleted + "]"
                + ", patient resources ["
                + " saved " + patientActuallySaved + "/" + patientTrySaved + ", deleted " + patientActuallyDeleted + "/" + patientTryDeleted + " over " + patientCount + " patients]");
    }


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
            set.add(ResourceType.QuestionnaireResponse);
            set.add(ResourceType.Composition);

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

    private void cachePatientDeleteState(UUID patientId, boolean isDeleted) {
        deletedPatientStateCache.put(patientId, Boolean.valueOf(isDeleted));
    }

    public boolean isPatientDeleted(String patientLocalId) throws Exception {
        UUID patientUuid = mapLocalIdToUuid(patientLocalId);
        return isPatientDeleted(patientUuid);
    }

    public boolean isPatientDeleted(UUID patientId) throws Exception {
        Boolean isDeleted = deletedPatientStateCache.get(patientId);
        if (isDeleted == null) {
            //if we've not cached it, then we'll need to hit the DB
            ResourceDalI dal = DalProvider.factoryResourceDal();
            //checksum will be null if the resource is deleted
            Long checksum = dal.getResourceChecksum(serviceId, ResourceType.Patient.toString(), patientId);
            isDeleted = Boolean.valueOf(checksum == null);
            deletedPatientStateCache.put(patientId, isDeleted);
        }
        return isDeleted.booleanValue();
    }

    /**
     * validates if a resource can be saved - we don't want to save resources for patients that have been deleted
     */
    private boolean validatePatientDeleteState(UUID patientId, boolean isDelete, ResourceBuilderBase resourceBuilder) throws Exception {

        if (isDelete //always let things be deleted
                || !isPatientDeleted(patientId)) {
            return true;

        } else {
            //LOG.warn("Ignoring save of " + resourceBuilder.getResource().getResourceType() + " " + resourceBuilder.getResourceId() + " because patient resource is deleted");
            TransformWarnings.log(LOG, this, "Ignoring save of {} {} because patient resource {} is deleted", resourceBuilder.getResource().getResourceType(), resourceBuilder.getResourceId(), patientId);
            return false;
        }
    }

    class ResourceJob {
        private CsvCurrentState parserState = null;
        private boolean isDelete = false;
        private ExchangeBatch exchangeBatch = null;
        private ResourceBuilderBase resourceBuilder = null;
        private boolean isDefinitelyNewResource = false;

        ResourceJob(CsvCurrentState parserState, boolean isDelete, ExchangeBatch exchangeBatch, ResourceBuilderBase resourceBuilder) {
            this.parserState = parserState;
            this.isDelete = isDelete;
            this.exchangeBatch = exchangeBatch;
            this.resourceBuilder = resourceBuilder;
        }

        public CsvCurrentState getParserState() {
            return parserState;
        }

        public boolean isDelete() {
            return isDelete;
        }

        public ExchangeBatch getExchangeBatch() {
            return exchangeBatch;
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

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (isDelete()) {
                sb.append("Delete Job ");
            } else {
                sb.append("Save Job ");
            }
            sb.append(" for ");
            sb.append(resourceBuilder.getResource().getResourceType());
            sb.append(" ");
            sb.append(resourceBuilder.getResource().getId());
            return sb.toString();
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

        public MapIdTask() {
        }

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

            for (ResourceJob job : jobs) {
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
                for (Resource resource : resources) {
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
            for (ResourceBuilderBase resourceBuilder : resourceBuilders) {
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
                    validator = (ResourceValidatorBase) cls.newInstance();
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
                String msg = "";
                if (deleting) {
                    msg = "Validation errors deleting " + resource.getResourceType() + " " + resource.getId();
                } else {
                    msg = "Validation errors saving " + resource.getResourceType() + " " + resource.getId();
                }
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
                //LOG.trace("In save resource task for " + jobs.size() + " jobs");
                Map<Resource, ExchangeBatch> hmResourcesAndBatches = new HashMap<>();
                Set<Resource> definitelyNewResources = new HashSet<>();
                Map<String, ResourceFieldMappingAudit> hmAuditsByResourceId = new HashMap<>();

                for (ResourceJob job : jobs) {
                    //LOG.trace("Saving " + job);
                    //save the exchange batch if it needs saving
                    ExchangeBatch exchangeBatch = job.getExchangeBatch();
                    ResourceBuilderBase builder = job.getResourceBuilder();
                    Resource resource = builder.getResource();

                    //apply any existing merge mappings to the resource - this is done here because not all resources
                    //go through the ID mapping process, but this needs doing every time
                    //merge map only applied to HL7 feed, so removed from this generic class
                    /*try {
                        Map<String, String> pastMergeReferences = ResourceMergeMapHelper.getResourceMergeMappings(serviceId);
                        IdHelper.applyExternalReferenceMappings(resource, pastMergeReferences, false);
                    } catch (Exception ex) {
                        LOG.error("", ex);
                        throw new FilingAndMappingException("Exception applying external reference mappings to " + resource.getResourceType() + " " + resource.getId(), job.getParserState(), ex);
                    }*/

                    hmResourcesAndBatches.put(resource, exchangeBatch);

                    ResourceFieldMappingAudit audit = builder.getAuditWrapper();
                    if (!audit.isEmpty()) {
                        String resourceId = resource.getId();
                        hmAuditsByResourceId.put(resourceId, audit);
                    }

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
                //LOG.trace("Done save and " + wrappersUpdated.size() + " wrappers were updated");
                //store our audit trail if we actually saved the resource
                SourceFileMappingDalI dal = DalProvider.factorySourceFileMappingDal();
                Map<ResourceWrapper, ResourceFieldMappingAudit> hmAuditsToSave = new HashMap<>();
                for (ResourceWrapper wrapper : wrappersUpdated) {
                    ResourceFieldMappingAudit audit = hmAuditsByResourceId.get(wrapper.getResourceId().toString());
                    if (audit != null) {
                        hmAuditsToSave.put(wrapper, audit);
                    }

                    //record that we've actually saved an updated/new resource
                    if (isDelete) {
                        countResourcesActuallyDeleted.get(wrapper.getExchangeBatchId()).incrementAndGet();
                    } else {
                        countResourcesActuallySaved.get(wrapper.getExchangeBatchId()).incrementAndGet();
                    }
                }

                dal.saveResourceMappings(hmAuditsToSave);

            } catch (Exception ex) {

                String err = "Exception filing resources: ";
                List<CsvCurrentState> parserStates = new ArrayList<>();
                StringBuilder sb = new StringBuilder();
                for (ResourceJob job : jobs) {
                    ResourceBuilderBase builder = job.getResourceBuilder();
                    Resource resource = builder.getResource();
                    err += resource.getResourceType() + "/" + resource.getId() + " ";

                    CsvCurrentState parserState = job.getParserState();
                    if (parserState == null) {
                        //if one of our jobs has a null parser state, then it's not a CSV-record level error
                        //so null the list so it gets treated as a fatal error
                        parserStates = null;

                    } else if (parserStates != null) {
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


    /**
     * returns the original date of the data in the exchange (i.e. when actually sent to DDS)
     */
    public Date getDataDate() throws Exception {
        if (cachedDataDate == null) {
            ExchangeDalI exchangeDal = DalProvider.factoryExchangeDal();
            Exchange x = exchangeDal.getExchange(exchangeId);
            cachedDataDate = x.getHeaderAsDate(HeaderKeys.DataDate);

            if (cachedDataDate == null) {
                throw new Exception("Failed to find data date for exchange " + exchangeId);
            }
        }
        return cachedDataDate;
    }

}
