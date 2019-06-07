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
    private Set<String> softwareFormatsToDrainQueueOnFailure;
    private String cernerPatientIdFile;
    private int maxTransformErrorsBeforeAbort;
    private List<Pattern> warningsToFailOn;
    private boolean disableSavingResources;
    private boolean validateResourcesOnSaving;
    private int resourceCacheMaxSizeInMemory;
    private String resourceCacheTempPath;
    private boolean isLive;
    private int resourceSaveBatchSize;
    private boolean allowMissingConceptIdsInSubscriberTransform;
    private Map<String, Set<String>> hmFileTypeFilters;
    private int rabbitMessagePerSecondThrottle;

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
        this.softwareFormatsToDrainQueueOnFailure = new HashSet<>();
        this.cernerPatientIdFile = null;
        this.maxTransformErrorsBeforeAbort = 50;
        this.warningsToFailOn = new ArrayList<>();
        this.disableSavingResources = false;
        this.validateResourcesOnSaving = true;
        this.resourceCacheMaxSizeInMemory = 100000;
        this.resourceCacheTempPath = null; //using null means we'll offload resources to the DB
        this.isLive = false;
        this.resourceSaveBatchSize = 50;
        this.hmFileTypeFilters = new HashMap<>();
        this.rabbitMessagePerSecondThrottle = 5000;

        try {
            JsonNode json = ConfigManager.getConfigurationAsJson("common_config", "queuereader");

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

            node = json.get("resource_cache_temp_dir");
            if (node != null) {
                this.resourceCacheTempPath = node.asText();
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

                JsonNode subNode = node.get("disabled_ods_codes_allowed");
                if (subNode != null) {
                    for (int i=0; i<subNode.size(); i++) {
                        String s = subNode.get(i).asText();
                        this.emisDisabledOdsCodesAllowed.add(s);
                    }
                }

                subNode = node.get("allow_missing_codes");
                if (subNode != null) {
                    this.emisAllowMissingCodes = subNode.asBoolean();
                }

                subNode = node.get("allow_unmapped_registration_types");
                if (subNode != null) {
                    this.emisAllowUnmappedRegistrationTypes = subNode.asBoolean();
                }
            }

            node = json.get("cerner");
            if (node != null) {
                /*JsonNode subNode = node.get("transform_2_1_files");
                if (subNode != null) {
                    this.transformCerner21Files = subNode.asBoolean();
                }*/

                JsonNode subNode = node.get("patient_id_file");
                if (subNode != null) {
                    this.cernerPatientIdFile = subNode.asText();
                }
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

            //ensure the temp path dir exists if set
            if (!Strings.isNullOrEmpty(resourceCacheTempPath)) {
                new File(resourceCacheTempPath).mkdirs();
            }

        } catch (Exception var4) {
            //if the config record is there, just log it out rather than throw an exception
            LOG.warn("No common queuereader config found in config DB with app_id queuereader and config_id common_config");
        }

        LOG.debug("resourceSaveBatchSize = " + resourceSaveBatchSize);
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

    public String getResourceCacheTempPath() {
        return resourceCacheTempPath;
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
}
