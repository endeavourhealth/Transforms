package org.endeavourhealth.transform.common;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.ExchangeBatchDalI;
import org.endeavourhealth.core.database.dal.audit.ExchangeDalI;
import org.endeavourhealth.core.database.dal.audit.models.Exchange;
import org.endeavourhealth.core.database.dal.audit.models.ExchangeBatch;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.transform.common.resourceBuilders.GenericBuilder;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.*;

public class PatientDeleteHelper {

    public static void deleteAllResourcesForPatient(String localPatientId, FhirResourceFiler fhirResourceFiler, CsvCurrentState currentState, CsvCell... sourceCells) throws Exception {

        //convert local ID to DDS UUID
        UUID serviceId = fhirResourceFiler.getServiceId();
        UUID edsPatientId = IdHelper.getEdsResourceId(serviceId, ResourceType.Patient, localPatientId);
        if (edsPatientId == null) {
            return;
        }

        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        ExchangeDalI exchangeDal = DalProvider.factoryExchangeDal();
        ExchangeBatchDalI exchangeBatchDal = DalProvider.factoryExchangeBatchDal();

        List<ResourceWrapper> resourceWrappers = resourceDal.getResourcesByPatient(serviceId, edsPatientId);

        //if re-processing old exchanges and filtering on specific file types, then we run the risk
        //of deleting resources for patients who were deleted and then came back - which is very
        //rare, but it has happened. Because we process this delete, and remove all FHIR resources, but then
        //DON'T re-process the other files that would have created those resources in the first place.
        //So we need to make sure to not delete any resources dated AFTER this delete of the patient.
        UUID currentExchangeId = fhirResourceFiler.getExchangeId();

        Exchange exchange = exchangeDal.getExchange(currentExchangeId);
        Date currentExchangeDate = exchange.getTimestamp(); //use exchange timestamp as that's ALWAYS present

        Map<UUID, Date> hmBatchDateCache = new HashMap<>();

        for (ResourceWrapper resourceWrapper: resourceWrappers) {

            //we need to find the latest exchange where this resource was updated
            String resourceType = resourceWrapper.getResourceType();
            UUID resourceId = resourceWrapper.getResourceId();
            List<ResourceWrapper> history = resourceDal.getResourceHistory(serviceId, resourceType, resourceId);

            //history is latest-first
            ResourceWrapper latest = history.get(0);
            UUID latestBatchId = latest.getExchangeBatchId();

            //cache for performance since many resources will be in the same batch
            Date latestDate = hmBatchDateCache.get(latestBatchId);
            if (latestDate == null) {
                ExchangeBatch batch = exchangeBatchDal.getForBatchId(latestBatchId);
                UUID latestExchangeId = batch.getExchangeId();
                Exchange latestExchange = exchangeDal.getExchange(latestExchangeId);
                latestDate = latestExchange.getTimestamp();
                hmBatchDateCache.put(latestBatchId, latestDate);
            }

            //if this resource was last updated by an exchange received AFTER the one currently being
            //processed, then do not delete it. There'll either be a later patient re-creation to make
            //those resources make sense, or a subsequent delete to delete them too.
            if (latestDate.after(currentExchangeDate)) {
                continue;
            }

            //wrap the resource in generic builder so we can delete it
            Resource resource = resourceWrapper.getResource();
            GenericBuilder genericBuilder = new GenericBuilder(resource);
            genericBuilder.setDeletedAudit(sourceCells);
            fhirResourceFiler.deletePatientResource(currentState, false, genericBuilder);
        }

    }
}
