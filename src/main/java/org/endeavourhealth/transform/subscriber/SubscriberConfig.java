package org.endeavourhealth.transform.subscriber;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.transform.subscriber.json.LinkDistributorConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SubscriberConfig {

    private static final int DEFAULT_TRANSFORM_BATCH_SIZE = 50;

    public enum SubscriberType {
        CompassV1,
        CompassV2
    }

    public enum CohortType {
        AllPatients,
        ExplicitPatients,
        GpRegisteredAt
    }

    public enum SubscriberLocation {
        Internal,
        Remote,
        InternalAndRemote
    }

    //common properties to subscribers
    private String subscriberConfigName;
    private SubscriberType subscriberType;
    private boolean includeDateRecorded;
    private int batchSize = DEFAULT_TRANSFORM_BATCH_SIZE;
    private String excludeNhsNumberRegex;
    private boolean excludeTestPatients;
    private boolean excludePatientsWithoutNhsNumber;
    private boolean isPseudonymised;
    private List<LinkDistributorConfig> pseudoSalts = new ArrayList<>();
    private List<LinkDistributorConfig> ralfSalts = new ArrayList<>();
    private CohortType cohortType;
    private Set<String> cohortGpServices = new HashSet<>(); //if cohort is GpRegisteredAt, this gives the ODS codes the patients should be registered at
    private Integer remoteSubscriberId;
    private String enterpriseServerUrl;
    private boolean includePatientAge;
    private String description; //textual description/reminder of what each config is for
    private SubscriberLocation subscriberLocation;
    private boolean includeObservationAdditional;

    //compass v1 properties

    //compass v2 properties

    public SubscriberConfig(String subscriberConfigName) {
        this.subscriberConfigName = subscriberConfigName;
    }


    public SubscriberType getSubscriberType() {
        return subscriberType;
    }

    public boolean isIncludeDateRecorded() {
        return includeDateRecorded;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getExcludeNhsNumberRegex() {
        return excludeNhsNumberRegex;
    }

    public boolean isExcludeTestPatients() {
        return excludeTestPatients;
    }

    public boolean isPseudonymised() {
        return isPseudonymised;
    }

    public boolean isExcludePatientsWithoutNhsNumber() {
        return excludePatientsWithoutNhsNumber;
    }

    public List<LinkDistributorConfig> getPseudoSalts() {
        return pseudoSalts;
    }

    public List<LinkDistributorConfig> getRalfSalts() {
        return ralfSalts;
    }

    public Integer getRemoteSubscriberId() {
        return remoteSubscriberId;
    }

    public CohortType getCohortType() {
        return cohortType;
    }

    public String getSubscriberConfigName() {
        return subscriberConfigName;
    }

    public Set<String> getCohortGpServices() {
        return cohortGpServices;
    }

    public String getEnterpriseServerUrl() {
        return enterpriseServerUrl;
    }

    public boolean isIncludePatientAge() {
        return includePatientAge;
    }

    public String getDescription() {
        return description;
    }

    public SubscriberLocation getSubscriberLocation() {
        return subscriberLocation;
    }

    public boolean isIncludeObservationAdditional() {
        return includeObservationAdditional;
    }

    public static SubscriberConfig readFromConfig(String subscriberConfigName) throws Exception {
        JsonNode config = ConfigManager.getConfigurationAsJson(subscriberConfigName, "db_subscriber");
        if (config == null) {
            throw new Exception("No config record found for [" + subscriberConfigName + "]");
        }
        return readFromJson(subscriberConfigName, config);
    }

    public static SubscriberConfig readFromJson(String subscriberConfigName, JsonNode config) throws Exception {

        SubscriberConfig ret = new SubscriberConfig(subscriberConfigName);
        ret.populateFromJson(config);
        return ret;
    }

    private void populateFromJson(JsonNode config) throws Exception {

        if (!config.has("subscriber_type")) {
            throw new Exception("[subscriber_type] element not found");
        }
        this.subscriberType = parseSubscriberType(config.get("subscriber_type").asText());

        if (config.has("transform_batch_size")) {
            this.batchSize = config.get("transform_batch_size").asInt();
        }

        if (config.has("excluded_nhs_number_regex")) {
            this.excludeNhsNumberRegex = config.get("excluded_nhs_number_regex").asText();
        }

        this.excludeTestPatients = config.has("exclude_test_patients")
                && config.get("exclude_test_patients").asBoolean();

        this.excludePatientsWithoutNhsNumber = config.has("exclude_no_nhs_number")
                && config.get("exclude_no_nhs_number").asBoolean();

        this.includeDateRecorded = config.has("include_date_recorded")
                && config.get("include_date_recorded").asBoolean();

        if (config.has("remote_subscriber_id")) {
            this.remoteSubscriberId = new Integer(config.get("remote_subscriber_id").asInt());
        }

        if (config.has("web_server")) {
            this.enterpriseServerUrl = config.get("web_server").asText();
        }

        if (!config.has("cohort_type")) {
            throw new Exception("[cohort_type] element not found");
        }
        this.cohortType = parseCohortType(config.get("cohort_type").asText());

        if (cohortType == CohortType.GpRegisteredAt) {
            JsonNode arr = config.get("cohort");
            for (int i=0; i<arr.size(); i++) {
                String odsCode = arr.get(i).asText();
                cohortGpServices.add(odsCode);
            }
        }

        this.isPseudonymised = config.has("pseudonymised")
                && config.get("pseudonymised").asBoolean();

        if (config.has("pseudo_salts")) {

            JsonNode arrayNode = config.get("pseudo_salts");
            String linkDistributors = convertJsonNodeToString(arrayNode);
            LinkDistributorConfig[] arr = ObjectMapperPool.getInstance().readValue(linkDistributors, LinkDistributorConfig[].class);

            for (LinkDistributorConfig l : arr) {
                this.pseudoSalts.add(l);
            }
        }

       if (config.has("ralf_salts")) {

            JsonNode arrayNode = config.get("ralf_salts");
            String linkDistributors = convertJsonNodeToString(arrayNode);
            LinkDistributorConfig[] arr = ObjectMapperPool.getInstance().readValue(linkDistributors, LinkDistributorConfig[].class);

            for (LinkDistributorConfig l : arr) {
                this.ralfSalts.add(l);
            }
        }

        this.includePatientAge = config.has("include_patient_age")
                && config.get("include_patient_age").asBoolean();

        if (config.has("description")) {
            this.description = config.get("description").asText();
        }

        if (config.has("subscriber_location")) {
            this.subscriberLocation = parseSubscriberLocation(config.get("subscriber_location").asText());
        }

        this.includeObservationAdditional = config.has("include_observation_additional")
                && config.get("include_observation_additional").asBoolean();

        //version-specific config
        if (subscriberType == SubscriberType.CompassV1) {

            //add v1 specific settings here

        } else if (subscriberType == SubscriberType.CompassV2) {

            //add v2 specific settings here

        } else {
            throw new Exception("No handler for subscriber type " + this.subscriberType);
        }

    }

    private static SubscriberLocation parseSubscriberLocation(String location) {
        if (location.equalsIgnoreCase("internal")) {
            return SubscriberLocation.Internal;

        } else if (location.equalsIgnoreCase("remote")) {
            return SubscriberLocation.Remote;

        } else if (location.equalsIgnoreCase("internal_and_remote")) {
            return SubscriberLocation.InternalAndRemote;

        } else {
            throw new RuntimeException("Unsupported subscriber location [" + location + "]");
        }
    }

    private static CohortType parseCohortType(String type) {

        if (type.equals("all_patients")) {
            return CohortType.AllPatients;

        } else if (type.equals("explicit_patients")) {
            return CohortType.ExplicitPatients;

        } else if (type.equals("gp_registered_at")) {
            return CohortType.GpRegisteredAt;

        } else {
            throw new RuntimeException("Unsupported cohort type [" + type + "]");
        }
    }

    private static String convertJsonNodeToString(JsonNode jsonNode) throws Exception {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Object json = mapper.readValue(jsonNode.toString(), Object.class);
            return mapper.writeValueAsString(json);
        } catch (Exception e) {
            throw new Exception("Error parsing Link Distributor Config");
        }
    }

    private static SubscriberType parseSubscriberType(String type) {
        if (type.equals("compass_v1")) {
            return SubscriberType.CompassV1;

        } else if (type.equals("compass_v2")) {
            return SubscriberType.CompassV2;

        } else {
            throw new RuntimeException("Unsupported subscriber type [" + type + "]");
        }
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        sb.append("subscriberConfigName = [" + subscriberConfigName + "],\r\n");
        sb.append("subscriberType = [" + subscriberType + "],\r\n");
        sb.append("isPseudonymised = [" + isPseudonymised + "],\r\n");
        sb.append("cohortType = [" + cohortType + "],\r\n");
        sb.append("cohortGpServices = [" + cohortGpServices.size() + "],\r\n");
        sb.append("pseudoSalts = [" + pseudoSalts.size() + "],\r\n");
        sb.append("ralfSalts = [" + ralfSalts.size() + "],\r\n");
        sb.append("excludeNhsNumberRegex = [" + excludeNhsNumberRegex + "],\r\n");
        sb.append("excludeTestPatients = [" + excludeTestPatients + "],\r\n");
        sb.append("excludePatientsWithoutNhsNumber = [" + excludePatientsWithoutNhsNumber + "],\r\n");
        sb.append("remoteSubscriberId = [" + remoteSubscriberId + "],\r\n");
        sb.append("includeDateRecorded = [" + includeDateRecorded + "],\r\n");
        sb.append("batchSize = [" + batchSize + "],\r\n");
        sb.append("enterpriseServerUrl = [" + enterpriseServerUrl + "],\r\n");
        sb.append("includePatientAge = [" + includePatientAge + "],\r\n");
        sb.append("description = [" + description + "],\r\n");
        sb.append("subscriberLocation = [" + subscriberLocation + "]");


        return sb.toString();
    }
}
