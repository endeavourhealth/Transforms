package org.endeavourhealth.transform.common;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.database.dal.eds.PatientSearchDalI;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.resourceBuilders.GenericBuilder;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class BulkDeleteTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(BulkDeleteTransformer.class);



    public static void transform(FhirResourceFiler fhirResourceFiler) throws Exception {

        UUID serviceId = fhirResourceFiler.getServiceId();
        Service service = DalProvider.factoryServiceDal().getById(serviceId);
        LOG.info("Performing bulk delete for patients at service " + service.toString());

        PatientSearchDalI patientSearchDal = DalProvider.factoryPatientSearchDal();
        List<UUID> patientUuids = patientSearchDal.getPatientIds(serviceId, true);
        LOG.info("Found " + patientUuids.size() + " to delete");

        int done = 0;

        for (UUID patientId: patientUuids) {

            ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
            List<ResourceWrapper> resourceWrappers = resourceRepository.getResourcesByPatient(serviceId, patientId);

            for (ResourceWrapper resourceWrapper: resourceWrappers) {
                String json = resourceWrapper.getResourceData();
                Resource resource = FhirSerializationHelper.deserializeResource(json);

                //wrap the resource in generic builder so we can delete it
                GenericBuilder genericBuilder = new GenericBuilder(resource);
                fhirResourceFiler.deletePatientResource(null, false, genericBuilder);
            }

            done ++;
            if (done % 100 == 0) {
                LOG.debug("Done " + done + " deletes");
            }
        }

        LOG.debug("Done " + done + " deletes");
        LOG.debug("Finished bulk delete of patient data at " + service.toString());
    }
}
