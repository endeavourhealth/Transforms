package org.endeavourhealth.transform.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import org.endeavourhealth.common.config.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;

public class TransformConfig {
    private static final Logger LOG = LoggerFactory.getLogger(TransformConfig.class);

    private String sharedStoragePath;
    private int attemptsPermmitedPerExchange;
    private String killFileLocation;
    private Set<String> emisDisabledOdsCodesAllowed;
    private boolean emisAllowMissingCodes;
    private boolean emisAllowUnmappedRegistrationTypes;
    private boolean emisSkipAdminData;
    private Set<String> softwareFormatsToDrainQueueOnFailure;
    private String cernerPatientIdFile;
    private int maxTransformErrorsBeforeAbort;
    private List<Pattern> warningsToFailOn;
    private boolean disableSavingResources;
    private boolean validateResourcesOnSaving;
    private int resourceCacheMaxSizeInMemory;
    private boolean isLive;
    private int resourceSaveBatchSize;
    private boolean allowMissingConceptIdsInSubscriberTransform;
    private Map<String, Set<String>> hmFileTypeFilters;
    private int rabbitMessagePerSecondThrottle;
    private Map<String, String> emisOdsCodesAndStartDates;
    private int adminBatchMaxSize;

    //singleton
    private static TransformConfig instance;
    private static Object sync = new Object();

    public static TransformConfig instance() {
        if (instance == null) {
            synchronized (sync) {
                if (instance == null) {
                    instance = new TransformConfig();
                }
            }
        }
        return instance;
    }

    private TransformConfig() {

        //set some defaults
        //this.sharedStoragePath = null; //can't really default this
        this.attemptsPermmitedPerExchange = 5;
        //this.killFileLocation = null; //no default
        this.emisDisabledOdsCodesAllowed = new HashSet<>();
        this.emisAllowMissingCodes = false;
        this.emisAllowUnmappedRegistrationTypes = false;
        this.emisSkipAdminData = false;
        this.softwareFormatsToDrainQueueOnFailure = new HashSet<>();
        this.cernerPatientIdFile = null;
        this.maxTransformErrorsBeforeAbort = 50;
        this.warningsToFailOn = new ArrayList<>();
        this.disableSavingResources = false;
        this.validateResourcesOnSaving = true;
        this.resourceCacheMaxSizeInMemory = 100000;
        this.isLive = false;
        this.resourceSaveBatchSize = 50;
        this.hmFileTypeFilters = new HashMap<>();
        this.rabbitMessagePerSecondThrottle = 5000;
        this.emisOdsCodesAndStartDates = new HashMap<>();
        this.adminBatchMaxSize = 30000; //largest known patient has 14k resources, so cap at double

        try {

            JsonNode json = ConfigManager.getConfigurationAsJson("common_config", "queuereader");
            loadCommonConfig(json);

            json = ConfigManager.getConfigurationAsJson("emis_config", "queuereader");
            loadEmisConfig(json);

            json = ConfigManager.getConfigurationAsJson("cerner_config", "queuereader");
            loadCernerConfig(json);

        } catch (Exception ex) {
            //if the config record is there, just log it out rather than throw an exception
            LOG.error("", ex);
        }

        LOG.debug("resourceSaveBatchSize = " + resourceSaveBatchSize);
    }

    private void loadCernerConfig(JsonNode json) {
        if (json == null) {
            return;
        }

        JsonNode node = json.get("patient_id_file");
        if (node != null) {
            this.cernerPatientIdFile = node.asText();
        }
    }

    private void loadCommonConfig(JsonNode json) throws Exception {

        if (json == null) {
            LOG.warn("No common queuereader config found in config DB with app_id queuereader and config_id common_config");
            return;
        }

        JsonNode node = json.get("shared_storage_path");
        if (node != null) {
            this.sharedStoragePath = node.asText();
        }

        node = json.get("disable_saving_resources");
        if (node != null) {
            this.disableSavingResources = node.asBoolean();
        }

        node = json.get("validate_resources_on_save");
        if (node != null) {
            this.validateResourcesOnSaving = node.asBoolean();
        }

        node = json.get("attempts_permmitted_per_exchange");
        if (node != null) {
            this.attemptsPermmitedPerExchange = node.asInt();
        }

        node = json.get("kill_file_location");
        if (node != null) {
            this.killFileLocation = node.asText();
        }

        node = json.get("shared_storage_path");
        if (node != null) {
            this.sharedStoragePath = node.asText();
        }

        node = json.get("transform_errors_before_abort");
        if (node != null) {
            this.maxTransformErrorsBeforeAbort = node.asInt();
        }

        node = json.get("resource_cache_max_size_in_memory");
        if (node != null) {
            this.resourceCacheMaxSizeInMemory = node.asInt();
        }

        node = json.get("is_live");
        if (node != null) {
            this.isLive = node.asBoolean();
        }

        node = json.get("resource_save_batch_size");
        if (node != null) {
            this.resourceSaveBatchSize = node.asInt();
        }

        node = json.get("rabbit_message_per_second_throttle");
        if (node != null) {
            this.rabbitMessagePerSecondThrottle = node.asInt();
        }

        node = json.get("emis");
        if (node != null) {
            //the emis config should now be in a separate config record, but still apply it if it's still found as part of the common config
            loadEmisConfig(node);
        }

        node = json.get("cerner");
        if (node != null) {
            //the cerner config should now be in a separate config record, but still apply it if it's still found as part of the common config
            loadCernerConfig(node);
        }

        node = json.get("subscriber");
        if (node != null) {

            JsonNode subNode = node.get("allow_missing_concepts");
            if (subNode != null) {
                this.allowMissingConceptIdsInSubscriberTransform = subNode.asBoolean();
            }
        }

        node = json.get("drain_queue_on_failure");
        if (node != null) {
            for (int i=0; i<node.size(); i++) {
                String software = node.get(i).asText();
                this.softwareFormatsToDrainQueueOnFailure.add(software);
            }
        }

        node = json.get("warnings_to_fail_on");
        if (node != null) {
            for (int i=0; i<node.size(); i++) {
                String s = node.get(i).asText();
                Pattern pattern = Pattern.compile(s);
                this.warningsToFailOn.add(pattern);
            }
        }

        node = json.get("file_type_filters");
        if (node != null) {
            for (int i=0; i<node.size(); i++) {
                JsonNode orgNode = node.get(i);
                JsonNode odsNode = orgNode.get("ods_code");
                JsonNode typesNode = orgNode.get("file_types");
                if (odsNode == null || typesNode == null) {
                    LOG.error("Missing ods_code or file_types node under file_type_filters node");
                    continue;
                }

                String odsCode = odsNode.asText();
                Set<String> fileTypes = new HashSet<>();

                for (int j=0; j<typesNode.size(); j++) {
                    String fileType = typesNode.get(j).asText();
                    fileTypes.add(fileType);
                }

                hmFileTypeFilters.put(odsCode, fileTypes);
            }
        }

        node = json.get("admin_batch_max_size");
        if (node != null) {
            this.adminBatchMaxSize = node.asInt();
        }
    }

    private void loadEmisConfig(JsonNode json) throws Exception {

        if (json == null) {
            return;
        }

        JsonNode subNode = json.get("disabled_ods_codes_allowed");
        if (subNode != null) {
            for (int i=0; i<subNode.size(); i++) {
                String s = subNode.get(i).asText();
                this.emisDisabledOdsCodesAllowed.add(s);
            }
        }

        subNode = json.get("allow_missing_codes");
        if (subNode != null) {
            this.emisAllowMissingCodes = subNode.asBoolean();
        }

        subNode = json.get("allow_unmapped_registration_types");
        if (subNode != null) {
            this.emisAllowUnmappedRegistrationTypes = subNode.asBoolean();
        }

        subNode = json.get("skip_admin_data");
        if (subNode != null) {
            this.emisSkipAdminData = subNode.asBoolean();
        }

        subNode = json.get("start_dates");
        if (subNode != null) {
            for (int i=0; i<subNode.size(); i++) {
                JsonNode orgNode = subNode.get(i);
                JsonNode odsNode = orgNode.get("ods_code");
                JsonNode startDateNode = orgNode.get("start_date");
                if (odsNode == null || startDateNode == null) {
                    LOG.error("Missing ods_code or start_date node under emis start_dates node");
                    continue;
                }

                String odsCode = odsNode.asText();
                String startDate = startDateNode.asText();
                emisOdsCodesAndStartDates.put(odsCode, startDate);
            }
        }




    }

    public String getSharedStoragePath() {
        return sharedStoragePath;
    }

    public int getAttemptsPermmitedPerExchange() {
        return attemptsPermmitedPerExchange;
    }

    public String getKillFileLocation() {
        return killFileLocation;
    }

    public Set<String> getEmisDisabledOdsCodesAllowed() {
        return emisDisabledOdsCodesAllowed;
    }

    public boolean isEmisAllowMissingCodes() {
        return emisAllowMissingCodes;
    }

    public boolean isEmisAllowUnmappedRegistrationTypes() {
        return emisAllowUnmappedRegistrationTypes;
    }

    public Set<String> getSoftwareFormatsToDrainQueueOnFailure() {
        return softwareFormatsToDrainQueueOnFailure;
    }

    public int getResourceCacheMaxSizeInMemory() {
        return resourceCacheMaxSizeInMemory;
    }

    public int getMaxTransformErrorsBeforeAbort() {
        return maxTransformErrorsBeforeAbort;
    }

    public List<Pattern> getWarningsToFailOn() {
        return warningsToFailOn;
    }

    public boolean isDisableSavingResources() {
        return disableSavingResources;
    }

    public boolean isValidateResourcesOnSaving() {
        return validateResourcesOnSaving;
    }

    public boolean isLive() {
        return isLive;
    }

    public int getResourceSaveBatchSize() {
        return resourceSaveBatchSize;
    }

    public String getCernerPatientIdFile() {
        return cernerPatientIdFile;
    }

    public boolean isAllowMissingConceptIdsInSubscriberTransform() {
        return allowMissingConceptIdsInSubscriberTransform;
    }

    public Set<String> getFilteredFileTypes(String odsCode) {
        return hmFileTypeFilters.get(odsCode);
    }

    public int getRabbitMessagePerSecondThrottle() {
        return rabbitMessagePerSecondThrottle;
    }

    public String getEmisStartDate(String odsCode) {
        return emisOdsCodesAndStartDates.get(odsCode);
    }

    public int getAdminBatchMaxSize() {
        return adminBatchMaxSize;
    }

    public boolean isEmisSkipAdminData() {
        return emisSkipAdminData;
    }
}
