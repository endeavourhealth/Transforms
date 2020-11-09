package org.endeavourhealth.transform.subscriber;

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

        SubscriberConfig subscriberConfig = SubscriberConfig.readFromConfig(subscriberConfigName);
        EnterpriseTransformHelper params = new EnterpriseTransformHelper(serviceUUID, null, null, batchUUID, subscriberConfig, resources, false);

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
        List<ResourceWrapper> copy = new ArrayList<>(resources);
        enterpriseTransformer.transformResources(copy,
                params.getOutputContainer().findCsvWriter(org.endeavourhealth.transform.enterprise.outputModels.Patient.class), params);


        List<String> filesToKeep = new ArrayList<>();
        filesToKeep.add("patient_address_match");
        params.getOutputContainer().clearDownOutputContainer(filesToKeep);

        byte[] bytes = params.getOutputContainer().writeToZip();

        if (bytes != null) {
            return Base64.getEncoder().encodeToString(bytes);
        } else {
            return null;
        }
    }

    public static String getSubscriberContainerForUPRNData(List<ResourceWrapper> resources, UUID serviceUUID, UUID batchUUID, String subscriberConfigName, UUID patientId) throws Exception {

        SubscriberConfig subscriberConfig = SubscriberConfig.readFromConfig(subscriberConfigName);
        SubscriberTransformHelper params = new SubscriberTransformHelper(serviceUUID, null, null, batchUUID, subscriberConfig, resources, false);

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
        filesToKeep.add(SubscriberTableId.PATIENT_ADDRESS_RALF);
        params.getOutputContainer().clearDownOutputContainer(filesToKeep);

        byte[] bytes = params.getOutputContainer().writeToZip();

        if (bytes != null) {
            return Base64.getEncoder().encodeToString(bytes);
        } else {
            return null;
        }
    }

    public static String getEnterpriseContainerForPatientData(List<ResourceWrapper> resources, UUID serviceUUID, UUID batchUUID, UUID protocolUUID, String subscriberConfigName, UUID patientId) throws Exception {

        SubscriberConfig subscriberConfig = SubscriberConfig.readFromConfig(subscriberConfigName);
        EnterpriseTransformHelper params = new EnterpriseTransformHelper(serviceUUID, null, null, batchUUID, subscriberConfig, resources, false);

        // Check if patient exists in target DB
        boolean patientFoundInSubscriber = checkIfPatientIsInEnterpriseDB(params, patientId.toString());

        if (!patientFoundInSubscriber) {
            LOG.info("Skipping patient " + patientId + " as not found in enterprise DB");
            return null;
        }

        Long enterpriseOrgId = FhirToEnterpriseCsvTransformer.findEnterpriseOrgId(serviceUUID, params);
        params.setEnterpriseOrganisationId(enterpriseOrgId);

        // take a copy of resources to avoid ConcurrentModificationException
        List<ResourceWrapper> copy = new ArrayList<>(resources);

        // // create a patient transformer and transform the Patient fhir resources
        AbstractEnterpriseTransformer enterprisePatientTransformer
                = FhirToEnterpriseCsvTransformer.createTransformerForResourceType(ResourceType.Patient);
        enterprisePatientTransformer.transformResources(copy,
                params.getOutputContainer().findCsvWriter(org.endeavourhealth.transform.enterprise.outputModels.Patient.class), params);

        List<String> filesToKeep = new ArrayList<>();
        filesToKeep.add(SubscriberTableId.PATIENT.getName());
        filesToKeep.add(SubscriberTableId.PATIENT_ADDRESS.getName());
        filesToKeep.add(SubscriberTableId.PATIENT_CONTACT.getName());

        params.getOutputContainer().clearDownOutputContainer(filesToKeep);

        byte[] bytes = params.getOutputContainer().writeToZip();
        if (bytes != null) {
            return Base64.getEncoder().encodeToString(bytes);
        } else {
            return null;
        }
    }

    public static String getEnterpriseContainerForEpisodeData(List<ResourceWrapper> resources, UUID serviceUUID, UUID batchUUID, UUID protocolUUID, String subscriberConfigName, UUID patientId) throws Exception {

        SubscriberConfig subscriberConfig = SubscriberConfig.readFromConfig(subscriberConfigName);
        EnterpriseTransformHelper params = new EnterpriseTransformHelper(serviceUUID, null, null, batchUUID, subscriberConfig, resources, false);

        // Check if patient exists in target DB
        boolean patientFoundInSubscriber = checkIfPatientIsInEnterpriseDB(params, patientId.toString());

        if (!patientFoundInSubscriber) {
            LOG.info("Skipping patient " + patientId + " episode of care as patient not found in enterprise DB");
            return null;
        }

        Long enterpriseOrgId = FhirToEnterpriseCsvTransformer.findEnterpriseOrgId(serviceUUID, params);
        params.setEnterpriseOrganisationId(enterpriseOrgId);

        //having done any patient resource in our batch, we should have created an enterprise patient ID and person ID that we can use for all remaining resources
        params.populatePatientAndPersonIds();

        // take a copy of resources to avoid ConcurrentModificationException
        List<ResourceWrapper> copy = new ArrayList<>(resources);

        // create a episode of care transformer and transform the fhir resources
        AbstractEnterpriseTransformer enterpriseEpisodeOfCareTransformer = FhirToEnterpriseCsvTransformer.createTransformerForResourceType(ResourceType.EpisodeOfCare);
        enterpriseEpisodeOfCareTransformer.transformResources(copy,
                params.getOutputContainer().findCsvWriter(org.endeavourhealth.transform.enterprise.outputModels.EpisodeOfCare.class), params);

        List<String> filesToKeep = new ArrayList<>();
        filesToKeep.add(SubscriberTableId.REGISTRATION_STATUS_HISTORY.getName());

        params.getOutputContainer().clearDownOutputContainer(filesToKeep);

        byte[] bytes = params.getOutputContainer().writeToZip();
        if (bytes != null) {
            return Base64.getEncoder().encodeToString(bytes);
        } else {
            return null;
        }
    }

    public static String getSubscriberContainerForPatientData(List<ResourceWrapper> resources, UUID serviceUUID, UUID batchUUID, UUID protocolUUID, String subscriberConfigName, UUID patientId) throws Exception {

        SubscriberConfig subscriberConfig = SubscriberConfig.readFromConfig(subscriberConfigName);
        SubscriberTransformHelper params = new SubscriberTransformHelper(serviceUUID, null, null, batchUUID, subscriberConfig, resources, false);

        // Check if patient exists in target DB
        boolean patientFoundInSubscriber = checkIfPatientIsInSubscriberDatabase(params, patientId.toString());

        if (!patientFoundInSubscriber) {
            LOG.info("Skipping patient " + patientId + " as not found in subscriber DB");
            return null;
        }

        Long enterpriseOrgId = FhirToSubscriberCsvTransformer.findEnterpriseOrgId(serviceUUID, params, Collections.emptyList());
        params.setSubscriberOrganisationId(enterpriseOrgId);

        // take a copy of resources to avoid ConcurrentModificationException
        List<ResourceWrapper> copy = new ArrayList<>(resources);

        // // create a patient transformer and transform the Patient fhir resources
        AbstractSubscriberTransformer subscriberTransformer = FhirToSubscriberCsvTransformer.createTransformerForResourceType(ResourceType.Patient);
        subscriberTransformer.transformResources(resources, params);

        List<SubscriberTableId> filesToKeep = new ArrayList<>();
        filesToKeep.add(SubscriberTableId.PATIENT);
        filesToKeep.add(SubscriberTableId.PATIENT_ADDRESS);
        filesToKeep.add(SubscriberTableId.PATIENT_CONTACT);

        params.getOutputContainer().clearDownOutputContainer(filesToKeep);

        byte[] bytes = params.getOutputContainer().writeToZip();
        if (bytes != null) {
            return Base64.getEncoder().encodeToString(bytes);
        } else {
            return null;
        }
    }

    public static String getSubscriberContainerForEpisodeData(List<ResourceWrapper> resources, UUID serviceUUID, UUID batchUUID, UUID protocolUUID, String subscriberConfigName, UUID patientId) throws Exception {

        SubscriberConfig subscriberConfig = SubscriberConfig.readFromConfig(subscriberConfigName);
        SubscriberTransformHelper params = new SubscriberTransformHelper(serviceUUID, null, null, batchUUID, subscriberConfig, resources, false);

        // Check if patient exists in target DB
        boolean patientFoundInSubscriber = checkIfPatientIsInSubscriberDatabase(params, patientId.toString());

        if (!patientFoundInSubscriber) {
            LOG.info("Skipping patient " + patientId + " as not found in subscriber DB");
            return null;
        }

        Long enterpriseOrgId = FhirToSubscriberCsvTransformer.findEnterpriseOrgId(serviceUUID, params, Collections.emptyList());
        params.setSubscriberOrganisationId(enterpriseOrgId);

        //having done any patient resource in our batch, we should have created an enterprise patient ID and person ID that we can use for all remaining resources
        params.populatePatientAndPersonIds();

        // take a copy of resources to avoid ConcurrentModificationException
        List<ResourceWrapper> copy = new ArrayList<>(resources);

        // create a episode of care transformer and transform the fhir resources
        AbstractSubscriberTransformer enterpriseEpisodeOfCareTransformer = FhirToSubscriberCsvTransformer.createTransformerForResourceType(ResourceType.EpisodeOfCare);
        enterpriseEpisodeOfCareTransformer.transformResources(copy, params);

        List<SubscriberTableId> filesToKeep = new ArrayList<>();
        filesToKeep.add(SubscriberTableId.REGISTRATION_STATUS_HISTORY);

        params.getOutputContainer().clearDownOutputContainer(filesToKeep);

        byte[] bytes = params.getOutputContainer().writeToZip();
        if (bytes != null) {
            return Base64.getEncoder().encodeToString(bytes);
        } else {
            return null;
        }
    }

    public static String getSubscriberContainerForObservationAdditionalData(List<ResourceWrapper> resources, UUID serviceUUID, UUID batchUUID, String subscriberConfigName, UUID patientId) throws Exception {

        SubscriberConfig subscriberConfig = SubscriberConfig.readFromConfig(subscriberConfigName);
        SubscriberTransformHelper params = new SubscriberTransformHelper(serviceUUID, null, UUID.randomUUID(), batchUUID, subscriberConfig, resources, false);
        params.populatePatientAndPersonIds();

        // Check if patient exists in target DB
        boolean patientFoundInSubscriber = checkIfPatientIsInSubscriberDatabase(params, patientId.toString());

        if (!patientFoundInSubscriber) {
            LOG.info("Skipping patient " + patientId + " as not found in subscriber DB");
            return null;
        }

        // create a patient transformer
        AbstractSubscriberTransformer subscriberTransformer = FhirToSubscriberCsvTransformer.createTransformerForResourceType(ResourceType.Observation);


        Long enterpriseOrgId = FhirToSubscriberCsvTransformer.findEnterpriseOrgId(serviceUUID, params, Collections.emptyList());
        params.setSubscriberOrganisationId(enterpriseOrgId);

        // transform the fhir resource
        subscriberTransformer.transformResources(resources, params);


        List<SubscriberTableId> filesToKeep = new ArrayList<>();
        filesToKeep.add(SubscriberTableId.OBSERVATION_ADDITIONAL);
        params.getOutputContainer().clearDownOutputContainer(filesToKeep);

        byte[] bytes = params.getOutputContainer().writeToZip();

        if (bytes != null) {
            return Base64.getEncoder().encodeToString(bytes);
        } else {
            return null;
        }
    }

    public static String getSubscriberContainerForConditionAdditionalData(List<ResourceWrapper> resources, UUID serviceUUID, UUID batchUUID, String subscriberConfigName, UUID patientId) throws Exception {

        SubscriberConfig subscriberConfig = SubscriberConfig.readFromConfig(subscriberConfigName);
        SubscriberTransformHelper params = new SubscriberTransformHelper(serviceUUID, null, UUID.randomUUID(), batchUUID, subscriberConfig, resources, false);
        params.populatePatientAndPersonIds();

        // Check if patient exists in target DB
        boolean patientFoundInSubscriber = checkIfPatientIsInSubscriberDatabase(params, patientId.toString());

        if (!patientFoundInSubscriber) {
            LOG.info("Skipping patient " + patientId + " as not found in subscriber DB");
            return null;
        }

        // create a patient transformer
        AbstractSubscriberTransformer subscriberTransformer = FhirToSubscriberCsvTransformer.createTransformerForResourceType(ResourceType.Condition);


        Long enterpriseOrgId = FhirToSubscriberCsvTransformer.findEnterpriseOrgId(serviceUUID, params, Collections.emptyList());
        params.setSubscriberOrganisationId(enterpriseOrgId);

        // transform the fhir resource
        subscriberTransformer.transformResources(resources, params);


        List<SubscriberTableId> filesToKeep = new ArrayList<>();
        filesToKeep.add(SubscriberTableId.OBSERVATION_ADDITIONAL);
        params.getOutputContainer().clearDownOutputContainer(filesToKeep);

        byte[] bytes = params.getOutputContainer().writeToZip();

        if (bytes != null) {
            return Base64.getEncoder().encodeToString(bytes);
        } else {
            return null;
        }
    }
}
