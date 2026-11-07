package org.endeavourhealth.transform.hl7v2fhir.helpers;

import ca.uhn.hl7v2.model.v23.datatype.CX;
import ca.uhn.hl7v2.model.v23.segment.MRG;
import ca.uhn.hl7v2.model.v23.segment.PID;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.utility.SlackHelper;
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


public class AcuteHL7Helper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(AcuteHL7Helper.class);
    private static Map<String, Map<String, String>> acuteCfgMap = null;

    //DB access
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
    private static final String ID_DELIMITER = ":";

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;
    private final String dataSharingAgreementGuid;
    private final Map<Class, AbstractCsvParser> parsers;

    public AcuteHL7Helper(UUID serviceId, UUID systemId, UUID exchangeId, String dataSharingAgreementGuid, Map<Class, AbstractCsvParser> parsers) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        this.dataSharingAgreementGuid = dataSharingAgreementGuid;
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

    /**
     *
     * @param locallyUniqueId
     * @param resourceType
     * @return
     * @throws Exception
     */
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

    /**
     *
     * @param resourceType
     * @param id
     * @return
     */
    public static String createResourceReference(String resourceType, String id) {
        return resourceType + "/" + id;
    }

    /**
     *
     * @param resourceType
     * @param id
     * @return
     */
    public static Reference createReference(ResourceType resourceType, String id) {
        return createReference(resourceType.toString(), id);
    }

    private static String getPatientIdentifier(PID pid, String subString) {
        String id = null;
        CX[] patientIdList = pid.getPatientIDInternalID();
        for (CX patient : patientIdList) {
            if(patient.getAssigningAuthority().getNamespaceID().toString().equals(subString)) {
                id = String.valueOf(patient.getID());
                break;
            }
        }
        return id;
    };

    public static String getPatientGUID(PID pid){
        return getPatientIdentifier(pid,"MRN");  // R1K
    }

    public static String getPatientNHSNumber(PID pid){
        return getPatientIdentifier(pid,"NHS");
    }

    public static List<String> getRemainingSourceids(PID pid, String resourceType) {
        List<String> MRNs = new ArrayList<>();
        CX[] patientIdList = pid.getPatientIDInternalID();
        for (CX patient : patientIdList) {
            if(patient.getAssigningAuthority().getNamespaceID().toString().equals("ALT_MRN") &&
                    !String.valueOf(patient.getID()).equals(getPatientGUID(pid))) {
                if(resourceType.equals("Patient")){
                    MRNs.add(String.valueOf((patient.getID())));
                }
                else {
                    MRNs.add((patient.getID())+ resourceType);
                }
            }
        }
        return MRNs;
    };

    public static CX getPatientReferenceSegment(PID pid, String subString) {
        CX patientReference = null;
        CX[] patientIdList = pid.getPatientIDInternalID();
        for (CX patient : patientIdList) {
            if(patient.getAssigningAuthority().getNamespaceID().toString().equals(subString)) {
                patientReference = patient;
                break;
            }
        }
        return patientReference;
    };

    public void MapRemainingSourceIdsWithEDSId(List<String> SourceIds, String EDSId, ResourceType resourceType) throws Exception {
        for (String SourceId : SourceIds) {
            IdHelper.getOrCreateEdsResourceId(serviceId, resourceType , SourceId, UUID.fromString(EDSId));
        }
    }


    public UUID mapLocalIdToEDSId(String localId, ResourceType resourceType) throws Exception {
        UUID EdsId = IdHelper.getOrCreateEdsResourceId(serviceId, resourceType, localId);
        return EdsId;
    }

    public static String getMergeMRN(MRG mrg, String subString) {
        String id = null;
        CX[] patientIdList = mrg.getPriorPatientIDInternal();
        for (CX patient : patientIdList) {
            if(patient.getAssigningAuthority().getNamespaceID().toString().equals(subString)) {
                id = String.valueOf(patient.getID());
                break;
            }
        }
        return id;
    };
    /**
     *
     * @param resourceType
     * @param id
     * @return
     */
    public static Reference createReference(String resourceType, String id) {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Blank id when creating reference for " + resourceType);
        } else {
            return createReference(createResourceReference(resourceType, id));
        }
    }

    /**
     *
     * @param referenceValue
     * @return
     */
    public static Reference createReference(String referenceValue) {
        return (new Reference()).setReference(referenceValue);
    }

    /**
     *
     * @param patientGuid
     * @return
     * @throws Exception
     */
    public static Reference createPatientReference(String patientGuid) throws Exception {
        return createReference(ResourceType.Patient, patientGuid);
    }

    /**
     *
     * @param organizationGuid
     * @return
     * @throws Exception
     */
    public static Reference createOrganizationReference(String organizationGuid) throws Exception {
        return createReference(ResourceType.Organization, organizationGuid);
    }

    /**
     *
     * @param practitionerGuid
     * @return
     * @throws Exception
     */
    public static Reference createPractitionerReference(String practitionerGuid) throws Exception {
        return createReference(ResourceType.Practitioner, practitionerGuid);
    }

    /**
     *
     * @param episodeOfCareGuid
     * @return
     * @throws Exception
     */
    public static Reference createEpisodeOfCareReference(String episodeOfCareGuid) throws Exception {
        return createReference(ResourceType.EpisodeOfCare, episodeOfCareGuid);
    }

    /**
     *
     * @param locationGuid
     * @return
     * @throws Exception
     */
    public static Reference createLocationReference(String locationGuid) throws Exception {
        return createReference(ResourceType.Location, locationGuid);
    }

    /**
     *
     * @param observationId
     * @return
     * @throws Exception
     */
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


    public static void raiseMissingAlert(String inboundCode, String category, String sourceSystem, String msgType, String edsUUID, String msgId, String mrnNo, AcuteHL7Helper acuteHL7Helper) throws Exception {

        if (inboundCode.startsWith ("CD:")) {
            LOG.info("Received incorrect alias of the " + inboundCode +  " value for " +category+ ". \n Message Type = "+ msgType +  ",\n Medical Record Number = "+ mrnNo + ",\n from Organization = " + sourceSystem + "\n with " +" Message ID = "+ msgId + " and \n Exchange ID = "+ edsUUID + ". Please fix accordingly.");
            String mappingError = inboundCode + " value for " +category+ ". Please fix accordingly.";
            TransformWarnings.log(LOG, acuteHL7Helper,
                    "Received incorrect alias for the  message type={}, Segment={}, MRN={}, MessageID={}",
                    msgType,mappingError,mrnNo,msgId);

            SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7MappingAlerts, "Received incorrect alias of the " + inboundCode +  " value for" +category+ ". \n Message Type = "+ msgType +  ",\n Medical Record Number = "+ mrnNo + ",\n from Organization = " + sourceSystem + "\n with " +" Message ID = "+ msgId + " and \n Exchange ID = "+ edsUUID + ". Please fix accordingly.");
        }
        else
            LOG.info("National mapping doesn't exist of the  " + inboundCode +  " value for  " +category+ " .\n Message Type = "+ msgType +  ",\n Medical Record Number = "+ mrnNo + ",\n from Organization = " + sourceSystem + " \n with " +" Message ID = "+ msgId + " and\n Exchange ID = "+ edsUUID + ". Please fix accordingly.");
        String mappingError = inboundCode + " value for " +category+ ". Please fix accordingly.";

        TransformWarnings.log(LOG, acuteHL7Helper,
                "National mapping doesn't exist for the  message type={}, Segment={}, MRN={}, MessageID={}",
                msgType,mappingError,mrnNo,msgId);

            SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7MappingAlerts, "National mapping doesn't exist of the  " + inboundCode +  " value for " +category+ " .\n Message Type = "+ msgType +  ",\n Medical Record Number = "+ mrnNo + ",\n from Organization = " + sourceSystem + " \n with " +" Message ID = "+ msgId + " and\n Exchange ID = "+ edsUUID + ". Please fix accordingly.");
    }

    public static MapResponse retrieveIMLookupMap(String odsCode, String codeStr, String code ) throws Exception {
        Map<String, String> acuteCfgMap=AcuteHL7Helper.getAcuteCfgMap(odsCode);
        MapResponse response=null;
      //IM Value Response
        if(null!=code){
            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    acuteCfgMap.get("provider"),acuteCfgMap.get("system"),null, null,
                    codeStr, code, acuteCfgMap.get(codeStr)
            );

            response = IMHelper.getIMMappedPropertyValueResponse(valueRequest);
        }
        //IM Property Response
        else
        {
            MapColumnRequest propertyRequest = new MapColumnRequest(
                    acuteCfgMap.get("provider"),acuteCfgMap.get("system"),null, null,codeStr
            );
            response = IMHelper.getIMMappedPropertyResponse(propertyRequest);
        }
        return response;

    }

    public static Map<String, String> getAcuteCfgMap(String source) {

        if (null==acuteCfgMap) {
            acuteCfgMap = new HashMap<String, Map<String, String>>();
            try {
                JsonNode configAcute = ConfigManager.getConfigurationAsJson("acute_im_configs");
                if (null != configAcute) {
                    JsonNode configs = configAcute.get("configs");
                    Iterator<JsonNode> configIter = configs.iterator();

                    while (configIter.hasNext()) {
                        JsonNode imConfig = configIter.next();
                        Map<String, String> configMap = new HashMap<String, String>();
                        configMap.put("provider", imConfig.get("provider").textValue());
                        configMap.put("system", imConfig.get("system").textValue());

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
                        acuteCfgMap.put(imConfig.get("source").textValue(), configMap);
                    }
                    return acuteCfgMap.get(source);
                }
            } catch (IOException e) {
                return null;
            }
        }
        return acuteCfgMap.get(source);
    }

}

