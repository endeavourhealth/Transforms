package org.endeavourhealth.transform.emis.csv.helpers;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisTransformDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisAdminResourceCache;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.common.resourceBuilders.GenericBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ResourceBuilderBase;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

public class EmisAdminCacheFiler {
    private static final Logger LOG = LoggerFactory.getLogger(EmisAdminCacheFiler.class);

    private static final EmisTransformDalI mappingRepository = DalProvider.factoryEmisTransformDal();

    private String dataSharingAgreementGuid = null;
    private ThreadPool threadPool = null;

    private List<EmisAdminResourceCache> resourcesToSave = new ArrayList<>();
    private List<EmisAdminResourceCache> resourcesToDelete = new ArrayList<>();
    private ReentrantLock lock = new ReentrantLock();

    public EmisAdminCacheFiler(String dataSharingAgreementGuid) throws Exception {
        this.dataSharingAgreementGuid = dataSharingAgreementGuid;

        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        this.threadPool = new ThreadPool(threadPoolSize, 50000, "EmisAdminCache");
    }

    public void close() throws Exception {

        if (!this.resourcesToSave.isEmpty()) {
            runPendingSaves();
        }
        if (!this.resourcesToDelete.isEmpty()) {
            runPendingDeletes();
        }

        List<ThreadPoolError> errors = threadPool.waitAndStop();
        handleErrors(errors);
    }

    public EmisAdminResourceCache getResourceFromCache(ResourceType resourceType, String sourceId) throws Exception {
        return mappingRepository.getAdminResource(this.dataSharingAgreementGuid, resourceType, sourceId);
    }

    public Map<String, EmisAdminResourceCache> getResourcesFromCache(ResourceType resourceType, List<String> sourceIds) throws Exception {
        return mappingRepository.getAdminResources(this.dataSharingAgreementGuid, resourceType, sourceIds);
    }

    private void runPendingSaves() throws Exception {

        List<EmisAdminResourceCache> copy;
        try {
            lock.lock();
            copy = new ArrayList<>(resourcesToSave);
            resourcesToSave.clear();

            SaveAdminTask task = new SaveAdminTask(copy, false);
            List<ThreadPoolError> errors = threadPool.submit(task);
            handleErrors(errors);
        } finally {
            lock.unlock();
        }
    }

    private void runPendingDeletes() throws Exception {
        List<EmisAdminResourceCache> copy;
        try {
            lock.lock();
            copy = new ArrayList<>(resourcesToDelete);
            resourcesToDelete.clear();

            SaveAdminTask task = new SaveAdminTask(copy, true);
            List<ThreadPoolError> errors = threadPool.submit(task);
            handleErrors(errors);
        } finally {
            lock.unlock();
        }
    }

    /**
     * we store a copy of all Organisations, Locations and Practitioner resources in a separate
     * table so that when new organisations are added to the extract, we can populate the db with
     * all those resources for the new org
     */
    public void saveAdminResourceToCache(ResourceBuilderBase resourceBuilder) throws Exception {

        Resource fhirResource = resourceBuilder.getResource();

        EmisAdminResourceCache cache = new EmisAdminResourceCache();
        cache.setDataSharingAgreementGuid(dataSharingAgreementGuid);
        cache.setResourceType(fhirResource.getResourceType().toString());
        cache.setEmisGuid(fhirResource.getId());
        cache.setResourceData(FhirSerializationHelper.serializeResource(fhirResource));
        cache.setAudit(resourceBuilder.getAuditWrapper());

        try {
            lock.lock();
            this.resourcesToSave.add(cache);

            if (this.resourcesToSave.size() > TransformConfig.instance().getResourceSaveBatchSize()) {
                runPendingSaves();
            }
        } finally {
            lock.unlock();
        }

    }


    public void deleteAdminResourceFromCache(ResourceBuilderBase resourceBuilder) throws Exception {

        Resource fhirResource = resourceBuilder.getResource();

        EmisAdminResourceCache cache = new EmisAdminResourceCache();
        cache.setDataSharingAgreementGuid(dataSharingAgreementGuid);
        cache.setResourceType(fhirResource.getResourceType().toString());
        cache.setEmisGuid(fhirResource.getId());

        try {
            lock.lock();
            this.resourcesToDelete.add(cache);

            if (this.resourcesToDelete.size() > TransformConfig.instance().getResourceSaveBatchSize()) {
                runPendingDeletes();
            }
        } finally {
            lock.unlock();
        }

    }

    private void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since the first exception is always most relevant
        ThreadPoolError first = errors.get(0);
        Throwable exception = first.getException();
        throw new TransformException("", exception);
    }

    /**
     * when we receive the first extract for an organisation, we need to copy all the contents of the admin
     * resource cache and save them against the new organisation. This is because EMIS only send most Organisations,
     * Locations and Staff once, with the very first organisation, and when a second organisation is added to
     * the extract, none of that data is re-sent, so we have to create those resources for the new org
     *
     * getting out of memory errors as there simply too many to retrieve into memory at once, so changing to stream them
     */
    public void applyAdminResourceCache(FhirResourceFiler fhirResourceFiler) throws Exception {

        mappingRepository.startRetrievingAdminResources(dataSharingAgreementGuid);

        int count = 0;

        while (true) {
            EmisAdminResourceCache cachedResource = mappingRepository.getNextAdminResource();
            if (cachedResource == null) {
                break;
            }

            //wrap the resource and audit trail in a generic resource builder for saving
            Resource fhirResource = FhirSerializationHelper.deserializeResource(cachedResource.getResourceData());
            ResourceFieldMappingAudit audit = cachedResource.getAudit();
            GenericBuilder genericBuilder = new GenericBuilder(fhirResource, audit);

            fhirResourceFiler.saveAdminResource(null, genericBuilder);

            //to cut memory usage, clear out the JSON field on each object as we pass it. Due to weird
            //Emis org/practitioner hierarchy, we've got 500k practitioners to save, so this is quite a lot of memory
            cachedResource.setDataSharingAgreementGuid(null);
            cachedResource.setEmisGuid(null);
            cachedResource.setResourceType(null);
            cachedResource.setResourceData(null);
            cachedResource.setAudit(null);

            //log progress
            count ++;
            if (count % 50000 == 0) {
                LOG.trace("Done " + count);
            }
        }

        LOG.trace("Finished " + count + " admin resources");
    }

    public boolean wasAdminCacheApplied(FhirResourceFiler fhirResourceFiler) throws Exception {
        return mappingRepository.wasAdminCacheApplied(fhirResourceFiler.getServiceId());
    }

    public void adminCacheWasApplied(FhirResourceFiler fhirResourceFiler) throws Exception {
        mappingRepository.adminCacheWasApplied(fhirResourceFiler.getServiceId(), dataSharingAgreementGuid);
    }

    static class SaveAdminTask implements Callable {

        private List<EmisAdminResourceCache> list;
        private boolean delete;

        public SaveAdminTask(List<EmisAdminResourceCache> list, boolean delete) {
            this.list = list;
            this.delete = delete;
        }

        @Override
        public Object call() throws Exception {
            try {
                if (delete) {
                    mappingRepository.deleteAdminResources(list);

                } else {
                    mappingRepository.saveAdminResources(list);
                }

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}
