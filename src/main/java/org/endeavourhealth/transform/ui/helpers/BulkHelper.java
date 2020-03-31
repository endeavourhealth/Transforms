package org.endeavourhealth.transform.ui.helpers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.XmlSerializer;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.LibraryDalI;
import org.endeavourhealth.core.database.dal.admin.models.ActiveItem;
import org.endeavourhealth.core.database.dal.admin.models.DefinitionItemType;
import org.endeavourhealth.core.database.dal.admin.models.Item;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.xml.QueryDocument.LibraryItem;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.FhirToEnterpriseCsvTransformer;
import org.endeavourhealth.transform.enterprise.transforms.AbstractEnterpriseTransformer;
import org.endeavourhealth.transform.subscriber.FhirToSubscriberCsvTransformer;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.endeavourhealth.transform.subscriber.transforms.AbstractSubscriberTransformer;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class BulkHelper {
    private static final Logger LOG = LoggerFactory.getLogger(BulkHelper.class);

    public static LibraryItem findProtocolLibraryItem(String protocolName) throws Exception{
        LibraryItem matchedLibraryItem = null;
        LibraryDalI repository = DalProvider.factoryLibraryDal();
        List<ActiveItem> activeItems = repository.getActiveItemByTypeId(Integer.valueOf(DefinitionItemType.Protocol.getValue()), Boolean.valueOf(false));
        for (ActiveItem activeItem : activeItems) {
            Item item = repository.getItemByKey(activeItem.getItemId(), activeItem.getAuditId());
            String xml = item.getXmlContent();
            LibraryItem libraryItem = (LibraryItem) XmlSerializer.deserializeFromString(LibraryItem.class, xml, (String) null);
            String name = libraryItem.getName();
            if (name.equals(protocolName)) {
                matchedLibraryItem = libraryItem;
                break;
            }
        }

        return matchedLibraryItem;
    }

    public static boolean checkIfPatientIsInEnterpriseDB(EnterpriseTransformHelper params, String discoveryPatientId) throws Exception {

        if (AbstractEnterpriseTransformer.findEnterpriseId(params, ResourceType.Patient.toString(), discoveryPatientId) == null) {
            return false;
        }

        return true;
    }

    public static boolean checkIfPatientIsInSubscriberDatabase(SubscriberTransformHelper params, String discoveryPatientId) throws Exception {

        String sourceId = ReferenceHelper.createResourceReference(ResourceType.Patient.toString(), discoveryPatientId);
        SubscriberId subscriberId = AbstractSubscriberTransformer.findSubscriberId(params, SubscriberTableId.PATIENT, sourceId);
        if (subscriberId == null) {
            return false;
        }

        return true;
    }

    public static String getEnterpriseContainerForUPRNData(List<ResourceWrapper> resources, UUID serviceUUID, UUID batchUUID, String subscriberConfigName, UUID patientId, String debug) throws Exception {

        EnterpriseTransformHelper params = new EnterpriseTransformHelper(serviceUUID, null, null, null, batchUUID, subscriberConfigName, resources, null);

        // Check if patient exists in target DB
        boolean patientFoundInSubscriber = checkIfPatientIsInEnterpriseDB(params, patientId.toString());

        if (!patientFoundInSubscriber) {
            LOG.info("Skipping patient " + patientId + " as not found in enterprise DB");
            return null;
        }

        if (debug.equals("1")) {
            LOG.info("*not* skipped");
            System.out.println("Press Enter key to continue...");
            Scanner scan = new Scanner(System.in);
            scan.nextLine();
        }

        // create a patient transformer
        AbstractEnterpriseTransformer enterpriseTransformer = FhirToEnterpriseCsvTransformer.createTransformerForResourceType(ResourceType.Patient);


        Long enterpriseOrgId = FhirToEnterpriseCsvTransformer.findEnterpriseOrgId(serviceUUID, params);
        params.setEnterpriseOrganisationId(enterpriseOrgId);

        // transform the fhir resource
        //enterpriseTransformer.transformResources(resources,
                //params.getOutputContainer().findCsvWriter(org.endeavourhealth.transform.enterprise.outputModels.Patient.class), params);

        // take a copy of resources to avoid ConcurrentModificationException
        List<ResourceWrapper> copy = new ArrayList(resources);
        enterpriseTransformer.transformResources(copy,
                params.getOutputContainer().findCsvWriter(org.endeavourhealth.transform.enterprise.outputModels.Patient.class), params);


        List<String> filesToKeep = new ArrayList<>();
        filesToKeep.add("patient_address_match");
        params.getOutputContainer().clearDownOutputContainer(filesToKeep);

        byte[] bytes = params.getOutputContainer().writeToZip();
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static String getSubscriberContainerForUPRNData(List<ResourceWrapper> resources, UUID serviceUUID, UUID batchUUID, String subscriberConfigName, UUID patientId) throws Exception {

        SubscriberTransformHelper params = new SubscriberTransformHelper(serviceUUID, null, null, null, batchUUID, subscriberConfigName, resources, null);

        // Check if patient exists in target DB
        boolean patientFoundInSubscriber = checkIfPatientIsInSubscriberDatabase(params, patientId.toString());

        if (!patientFoundInSubscriber) {
            LOG.info("Skipping patient " + patientId + " as not found in subscriber DB");
            return null;
        }

        // create a patient transformer
        AbstractSubscriberTransformer subscriberTransformer = FhirToSubscriberCsvTransformer.createTransformerForResourceType(ResourceType.Patient);


        Long enterpriseOrgId = FhirToSubscriberCsvTransformer.findEnterpriseOrgId(serviceUUID, params, Collections.emptyList());
        params.setSubscriberOrganisationId(enterpriseOrgId);

        // transform the fhir resource
        subscriberTransformer.transformResources(resources, params);


        List<SubscriberTableId> filesToKeep = new ArrayList<>();
        filesToKeep.add(SubscriberTableId.PATIENT_ADDRESS_MATCH);
        params.getOutputContainer().clearDownOutputContainer(filesToKeep);

        byte[] bytes = params.getOutputContainer().writeToZip();
        return Base64.getEncoder().encodeToString(bytes);

    }

}
