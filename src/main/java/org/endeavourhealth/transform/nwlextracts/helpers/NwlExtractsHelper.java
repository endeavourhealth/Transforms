package org.endeavourhealth.transform.nwlextracts.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.im.models.mapping.MapColumnRequest;
import org.endeavourhealth.im.models.mapping.MapColumnValueRequest;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.HasServiceSystemAndExchangeIdI;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.ResourceBuilderBase;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


public class NwlExtractsHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(NwlExtractsHelper.class);
    private static Map<String, Map<String, String>> cdeCfgMap = null;

    //DB access
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
    private static final String ID_DELIMITER = ":";

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;
    private final Map<Class, AbstractCsvParser> parsers;

    public NwlExtractsHelper(UUID serviceId, UUID systemId, UUID exchangeId, Map<Class, AbstractCsvParser> parsers) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        this.parsers = parsers;
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


    public Resource retrieveResource(String locallyUniqueId, ResourceType resourceType) throws Exception {
        UUID globallyUniqueId = IdHelper.getEdsResourceId(serviceId, resourceType, locallyUniqueId);

        //if we've never mapped the local ID to a EDS UI, then we've never heard of this resource before
        if (globallyUniqueId == null) {
            return null;
        }

        ResourceWrapper resourceHistory = resourceRepository.getCurrentVersion(serviceId, resourceType.toString(), globallyUniqueId);

        //if the resource has been deleted before, we'll have a null entry or one that says it's deleted
        if (resourceHistory == null
                || resourceHistory.isDeleted()) {
            return null;
        }

        String json = resourceHistory.getResourceData();
        try {
            return FhirSerializationHelper.deserializeResource(json);
        } catch (Throwable t) {
            throw new Exception("Error deserialising " + resourceType + " " + globallyUniqueId + " (raw ID " + locallyUniqueId + ")", t);
        }
    }

    public static String createResourceReference(String resourceType, String id) {
        return resourceType + "/" + id;
    }

    public static Reference createReference(ResourceType resourceType, String id) {
        return createReference(resourceType.toString(), id);
    }

    public static String getPatientGUID(List<String> PatientIdList){
        return PatientIdList.get(0);
    }


    public static List<String> getRemainingSourceids(List<String> patientIdList, String resourceType) {
        String patientGuid=getPatientGUID(patientIdList);
        List<String> uniqueIds = new ArrayList<>();
        for (String patient : patientIdList) {
            if(!patient.equals(patientGuid)) {
                if(resourceType.equals("Patient")){
                    uniqueIds.add(patient);
                }
                else {
                    uniqueIds.add(patient+ resourceType);
                }
            }
        }
        return uniqueIds;
    }

    public void MapRemainingSourceIdsWithEDSId(List<String> SourceIds, String EDSId, ResourceType resourceType) throws Exception {
        for (String SourceId : SourceIds) {
            IdHelper.getOrCreateEdsResourceId(serviceId, resourceType , SourceId, UUID.fromString(EDSId));
        }
    }


    public UUID mapLocalIdToEDSId(String localId, ResourceType resourceType) throws Exception {
        UUID edsId = IdHelper.getOrCreateEdsResourceId(serviceId, resourceType, localId);
        return edsId;
    }

    public static Reference createReference(String resourceType, String id) {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Blank id when creating reference for " + resourceType);
        } else {
            return createReference(createResourceReference(resourceType, id));
        }
    }

    public static Reference createReference(String referenceValue) {
        return (new Reference()).setReference(referenceValue);
    }

    public static Reference createPatientReference(String patientGuid) throws Exception {
        return createReference(ResourceType.Patient, patientGuid);
    }

    public static Reference createOrganizationReference(String organizationGuid) throws Exception {
        return createReference(ResourceType.Organization, organizationGuid);
    }

    public static Reference createPractitionerReference(String practitionerGuid) throws Exception {
        return createReference(ResourceType.Practitioner, practitionerGuid);
    }

    public static Reference createEpisodeOfCareReference(String episodeOfCareGuid) throws Exception {
        return createReference(ResourceType.EpisodeOfCare, episodeOfCareGuid);
    }

    public static Reference createLocationReference(String locationGuid) throws Exception {
        return createReference(ResourceType.Location, locationGuid);
    }

    public static Reference createObservationReference(String observationId) throws Exception {
        return createReference(ResourceType.Observation, observationId);
    }

    /**
     * to ensure globally unique IDs for all resources, a new ID is created
     * from the patientGuid and sourceGuid (e.g. observationGuid)
     */
    public static String createUniqueId(String patientGuid, String sourceGuid) {
        if (sourceGuid == null) {
            return patientGuid;
        } else {
            return patientGuid + ID_DELIMITER + sourceGuid;
        }
    }

    public Resource retrieveResourceForLocalId(ResourceType resourceType, String locallyUniqueId) throws Exception {

        UUID globallyUniqueId = IdHelper.getEdsResourceId(serviceId, resourceType, locallyUniqueId);

        //if we've never mapped the local ID to a EDS UI, then we've never heard of this resource before
        if (globallyUniqueId == null) {
            return null;
        }
        return retrieveResourceForUuid(resourceType, globallyUniqueId);
    }

    public Resource retrieveResourceForUuid(ResourceType resourceType, UUID resourceId) throws Exception {

        ResourceWrapper resourceHistory = resourceRepository.getCurrentVersion(serviceId, resourceType.toString(), resourceId);

        //if the resource has been deleted before, we'll have a null entry or one that says it's deleted
        if (resourceHistory == null
                || resourceHistory.isDeleted()) {
            return null;
        }

        String json = resourceHistory.getResourceData();
        try {
            return FhirSerializationHelper.deserializeResource(json);
        } catch (Throwable t) {
            throw new Exception("Error deserialising " + resourceType + " " + resourceId, t);
        }
    }

    public static void setUniqueId(ResourceBuilderBase resourceBuilder, String patientGuid, String sourceGuid) {
        String resourceId = createUniqueId(patientGuid, sourceGuid);
        resourceBuilder.setId(resourceId);
    }


    public static void raiseMissingAlert(String inboundCode, String category, String sourceSystem, String edsUUID, String patientID, NwlExtractsHelper nwlextractsHelper) throws Exception {

        if (inboundCode.startsWith ("CD:")) {
            LOG.info("Received incorrect alias of the " + inboundCode +  " value for " +category+ ",\n Patient ID = "+ patientID + ",\n from Organization = " + sourceSystem + "\n with " + " and \n Exchange ID = "+ edsUUID + ". Please fix accordingly.");
            String mappingError = inboundCode + " value for " +category+ ". Please fix accordingly.";
            TransformWarnings.log(LOG, nwlextractsHelper,
                    "Received incorrect alias for the  message type={}, Segment={}, MRN={}, MessageID={}",
                    mappingError,patientID);

           // SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7MappingAlerts, "Received incorrect alias of the " + inboundCode +  " value for" +category+ ",\n Patient ID = "+ patientID + ",\n from Organization = " + sourceSystem + "\n with " + "  \n Exchange ID = "+ edsUUID + ". Please fix accordingly.");
        }
        else
            LOG.info("National mapping doesn't exist of the  " + inboundCode +  " value for  " +category+ ",\n Patient ID = "+ patientID + ",\n from Organization = " + sourceSystem + " \n with " + " and\n Exchange ID = "+ edsUUID + ". Please fix accordingly.");
        String mappingError = inboundCode + " value for " +category+ ". Please fix accordingly.";

        TransformWarnings.log(LOG, nwlextractsHelper,
                "National mapping doesn't exist for the  message type={}, Segment={}, MRN={}, MessageID={}",mappingError,patientID);

           // SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7MappingAlerts, "National mapping doesn't exist of the  " + inboundCode +  " value for " +category+ ",\n Patient ID = "+ patientID + ",\n from Organization = " + sourceSystem + " \n with " + " \n Exchange ID = "+ edsUUID + ". Please fix accordingly.");
    }

    public static MapResponse retrieveIMLookupMap(String odsCode, String codeStr, String code ) throws Exception {
        Map<String, String> cdeCfgMap= NwlExtractsHelper.getCdeCfgMap(odsCode);
        MapResponse response = null;
        if (null!=cdeCfgMap) {
            //IM Value Response
            if (null != code) {
                MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                        cdeCfgMap.get("provider"), cdeCfgMap.get("system"), cdeCfgMap.get("schema"), null,
                        codeStr, code, cdeCfgMap.get(codeStr)
                );

                response = IMHelper.getIMMappedPropertyValueResponse(valueRequest);
            }
            //IM Property Response
            else {
                MapColumnRequest propertyRequest = new MapColumnRequest(
                        cdeCfgMap.get("provider"), cdeCfgMap.get("system"), cdeCfgMap.get("schema"), null, codeStr
                );
                response = IMHelper.getIMMappedPropertyResponse(propertyRequest);
            }
        }
        return response;
    }

    public static Map<String, String> getCdeCfgMap(String source) {

        if (null==cdeCfgMap) {
            cdeCfgMap = new HashMap<String, Map<String, String>>();
            try {
                JsonNode configCDE = ConfigManager.getConfigurationAsJson("cde_im_configs");
                if (null != configCDE) {
                    JsonNode configs = configCDE.get("configs");
                    Iterator<JsonNode> configIter = configs.iterator();

                    while (configIter.hasNext()) {
                        JsonNode imConfig = configIter.next();
                        Map<String, String> configMap = new HashMap<String, String>();
                        configMap.put("provider", imConfig.get("provider").textValue());
                        configMap.put("system", imConfig.get("system").textValue());
                        configMap.put("schema", imConfig.get("schema").textValue());

                        JsonNode schemeConf = imConfig.get("scheme");
                        Iterator<JsonNode> schemeConfigIter = schemeConf.iterator();
                        if (schemeConfigIter.hasNext()) {
                            JsonNode schemeConfig = schemeConfigIter.next();
                            Iterator<String> schemeNameItr = schemeConfig.fieldNames();
                            while (schemeNameItr.hasNext()){
                                String schemeName=schemeNameItr.next();
                                configMap.put(schemeName, schemeConfig.get(schemeName).textValue());
                            }
                        }
                        cdeCfgMap.put(imConfig.get("source").textValue(), configMap);
                    }
                    return cdeCfgMap.get(source);
                }
            } catch (IOException e) {
                return null;
            }
        }
        return cdeCfgMap.get(source);
    }

}

