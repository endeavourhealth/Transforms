package org.endeavourhealth.transform.emis.csv.helpers;

import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisTransformDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisAdminResourceCache;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.resourceBuilders.ResourceBuilderBase;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

public class EmisAdminCacheFiler {
    private static final Logger LOG = LoggerFactory.getLogger(EmisAdminCacheFiler.class);

    private static final ParserPool parser = new ParserPool();
    private static final EmisTransformDalI mappingRepository = DalProvider.factoryEmisTransformDal();

    private String dataSharingAgreementGuid = null;
    private ThreadPool threadPool = null;

    public EmisAdminCacheFiler(String dataSharingAgreementGuid) throws Exception {
        this.dataSharingAgreementGuid = dataSharingAgreementGuid;

        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        this.threadPool = new ThreadPool(threadPoolSize, 50000);
    }

    public void close() throws Exception {
        List<ThreadPoolError> errors = threadPool.waitAndStop();
        handleErrors(errors);
    }

    /**
     * we store a copy of all Organisations, Locations and Practitioner resources in a separate
     * table so that when new organisations are added to the extract, we can populate the db with
     * all those resources for the new org
     */
    public void saveAdminResourceToCache(CsvCurrentState parserState, ResourceBuilderBase resourceBuilder) throws Exception {

        Resource fhirResource = resourceBuilder.getResource();

        EmisAdminResourceCache cache = new EmisAdminResourceCache();
        cache.setDataSharingAgreementGuid(dataSharingAgreementGuid);
        cache.setResourceType(fhirResource.getResourceType().toString());
        cache.setEmisGuid(fhirResource.getId());
        cache.setResourceData(parser.composeString(fhirResource));
        cache.setAudit(resourceBuilder.getAuditWrapper());

        SaveAdminTask task = new SaveAdminTask(cache, false, parserState);
        List<ThreadPoolError> errors = threadPool.submit(task);
        handleErrors(errors);
    }

    public void deleteAdminResourceFromCache(CsvCurrentState parserState, ResourceBuilderBase resourceBuilder) throws Exception {

        Resource fhirResource = resourceBuilder.getResource();

        EmisAdminResourceCache cache = new EmisAdminResourceCache();
        cache.setDataSharingAgreementGuid(dataSharingAgreementGuid);
        cache.setResourceType(fhirResource.getResourceType().toString());
        cache.setEmisGuid(fhirResource.getId());

        SaveAdminTask task = new SaveAdminTask(cache, true, parserState);
        List<ThreadPoolError> errors = threadPool.submit(task);
        handleErrors(errors);
    }

    private void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since the first exception is always most relevant
        ThreadPoolError first = errors.get(0);
        SaveAdminTask callable = (SaveAdminTask)first.getCallable();
        Throwable exception = first.getException();
        CsvCurrentState parserState = callable.getParserState();
        throw new TransformException(parserState.toString(), exception);
    }

    static class SaveAdminTask implements Callable {

        private EmisAdminResourceCache cacheObj;
        private boolean delete;
        private CsvCurrentState parserState;

        public SaveAdminTask(EmisAdminResourceCache cacheObj, boolean delete, CsvCurrentState parserState) {
            this.cacheObj = cacheObj;
            this.delete = delete;
            this.parserState = parserState;
        }

        @Override
        public Object call() throws Exception {
            try {
                if (delete) {
                    mappingRepository.delete(cacheObj);

                } else {
                    mappingRepository.save(cacheObj);
                }

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }

        public CsvCurrentState getParserState() {
            return parserState;
        }
    }
}
