package org.endeavourhealth.transform.subscriber;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.transform.subscriber.json.LinkDistributorConfig;

import java.util.*;

public class SubscriberConfig {

    private static final int DEFAULT_TRANSFORM_BATCH_SIZE = 50;

    public enum SubscriberType {
        CompassV1,
        CompassV2
    }

    public enum CohortType {
        AllPatients,
        ExplicitPatients,
        GpRegisteredAt,
        ActiveOnlyGpRegisteredAt
    }

    public enum CohortPersistence {
        Retain,
        Remove
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
    private CohortPersistence cohortPersistence;
    //if cohort is GpRegisteredAt or ActiveOnlyGpRegisteredAt, this gives the ODS codes the patients should be registered at
    private Set<String> cohortGpServices = new HashSet<>();
    private Integer remoteSubscriberId;
    private String enterpriseServerUrl;
    private boolean includePatientAge;
    private String description; //textual description/reminder of what each config is for
    private SubscriberLocation subscriberLocation;
    private boolean includeObservationAdditional;
    private boolean includeAppointmentAdditional;   //SD-796
    private boolean includeScheduleAdditional;
    private boolean includeReferralRequestAdditional;
    private Integer minPatientAgeYears;    //the minimum age of the patient in years to be included, to support 18+ for example
    private boolean includePublisherMetaData;
    private String imRestrictedValueSet;   //the IM value set identifier used to restrict concepts being sent to the subscriber, non medication
    private String imRestrictedMedicationsValueSet;  //similar to imRestrictedValueSet but contain medication codes only
    private String activeOnlyIncludeEndedAfterDate;   //the date (format dd/MM/yyyy which any non active registrations, i.e. left /dead are retained / not excluded)
    private String dateOfBirthMask;   //masking format for the date of birth (format dd/MM/yyyy, i.e.  01/01/yyyy)
    private Map<String, List<String>> excludedTableColumns = new HashMap<>();  //excluded table columns for blanking at subscriber transform upsert time
    //private Map<String, List<String>> optOutType1Codes = new HashMap<>();  //opt out type 1 codes

    private Set<String> cohortInclusionSnomedConcepts = new HashSet<>();  //for cohort inclusion, i.e. Diabetic

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

    public Integer getMinPatientAgeYears() {
        return minPatientAgeYears;
    }

    public Integer getRemoteSubscriberId() {
        return remoteSubscriberId;
    }

    public CohortType getCohortType() {
        return cohortType;
    }

    public CohortPersistence getCohortPersistence() {
        return cohortPersistence;
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

    public boolean isIncludeAppointmentAdditional() {
        return includeAppointmentAdditional;
    }

    public boolean isIncludeScheduleAdditional() {
        return includeScheduleAdditional;
    }
    public boolean isIncludeReferralRequestAdditional() {
        return includeReferralRequestAdditional;
    }

    public boolean isIncludePublisherMetaData() {
        return includePublisherMetaData;
    }

    public String getImRestrictedValueSet() {
        return imRestrictedValueSet;
    }

    public String getImRestrictedMedicationsValueSet() {
        return imRestrictedMedicationsValueSet;
    }

    public String getActiveOnlyIncludeEndedAfterDate() {
        return activeOnlyIncludeEndedAfterDate;
    }

    public String getDateOfBirthMask() {
        return dateOfBirthMask;
    }

    public Map<String, List<String>> getExcludedTableColumns() {
        return excludedTableColumns;
    }

    //public Map<String, List<String>> getOptOutType1Codes() {return optOutType1Codes;}

    public Set<String> getCohortInclusionSnomedConcepts() {
        return cohortInclusionSnomedConcepts;
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

        if (config.has("cohort_persistence")) {
            this.cohortPersistence = parseCohortPersistence(config.get("cohort_persistence").asText());
        } else {
            this.cohortPersistence = CohortPersistence.Remove;
        }

        if (cohortType == CohortType.GpRegisteredAt || cohortType == CohortType.ActiveOnlyGpRegisteredAt) {
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

        this.includeAppointmentAdditional = config.has("include_appointment_additional")
                && config.get("include_appointment_additional").asBoolean();

        this.includeScheduleAdditional = config.has("include_schedule_additional")
                && config.get("include_schedule_additional").asBoolean();

        this.includeReferralRequestAdditional = config.has("include_referral_request_additional")
                && config.get("include_referral_request_additional").asBoolean();

        if (config.has("min_patient_age_years")) {
            this.minPatientAgeYears = config.get("min_patient_age_years").asInt();
        }

        this.includePublisherMetaData = config.has("include_publisher_meta_data")
                && config.get("include_publisher_meta_data").asBoolean();

        if (config.has ("im_restricted_value_set")) {
            this.imRestrictedValueSet = config.get("im_restricted_value_set").asText();
        }

        if (config.has ("im_restricted_medications_value_set")) {
            this.imRestrictedMedicationsValueSet = config.get("im_restricted_medications_value_set").asText();
        }

        if (config.has ("active_only_include_ended_after_date")) {
            this.activeOnlyIncludeEndedAfterDate = config.get("active_only_include_ended_after_date").asText();
        }

        if (config.has ("date_of_birth_mask")) {
            this.dateOfBirthMask = config.get("date_of_birth_mask").asText();
        }

        if (config.has("excluded_table_columns")) {

            JsonNode arrayNodes = config.get("excluded_table_columns");
            for (JsonNode tableNode : arrayNodes) {

                Iterator<Map.Entry<String, JsonNode>> iter = tableNode.fields();
                while (iter.hasNext()) {
                    Map.Entry<String, JsonNode> entry = iter.next();
                    String tableName = entry.getKey();              //table name
                    JsonNode columnNodes = entry.getValue();        //column(s) as JsonNode(s)
                    List<String> tableColumns = new ArrayList();
                    for (JsonNode columnNode : columnNodes) {
                        String columnName = columnNode.asText();    //column
                        tableColumns.add(columnName);
                    }
                    excludedTableColumns.put(tableName, tableColumns);
                }
            }
        }

        if (config.has("cohort_inclusion_snomed_concepts")) {

            JsonNode arr = config.get("cohort_inclusion_snomed_concepts");
            for (int i=0; i<arr.size(); i++) {
                String conceptCode = arr.get(i).asText();
                cohortInclusionSnomedConcepts.add(conceptCode);
            }
        }

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

        } else if (type.equals("active_only_gp_registered_at")) {
            return CohortType.ActiveOnlyGpRegisteredAt;

        } else {
            throw new RuntimeException("Unsupported cohort type [" + type + "]");
        }
    }

    private static CohortPersistence parseCohortPersistence(String persistence) {

        if (persistence.equals("retain")) {
            return CohortPersistence.Retain;

        } else if (persistence.equals("remove")) {
            return CohortPersistence.Remove;

        } else {
            throw new RuntimeException("Unsupported cohort persistence [" + persistence + "]");
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
        sb.append("cohortPersistence = [" + cohortPersistence + "],\r\n");
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
        sb.append("subscriberLocation = [" + subscriberLocation + "],\r\n");
        sb.append("activeOnlyIncludeEndedAfterDate = [" + activeOnlyIncludeEndedAfterDate + "],\r\n");
        sb.append("dateOfBirthMask = [" + dateOfBirthMask + "],\r\n");
        sb.append("minPatientAgeYears = [" + minPatientAgeYears + "]");

        return sb.toString();
    }
}
