package org.endeavourhealth.transform.common;

import com.fasterxml.jackson.databind.JsonNode;
import org.endeavourhealth.common.config.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class TransformConfig {
    private static final Logger LOG = LoggerFactory.getLogger(TransformConfig.class);

    private String sharedStoragePath;
    private int attemptsPermmitedPerExchange;
    private String killFileLocation;
    private List<Pattern> emisDisabledOragnisationsAllowed = new ArrayList<>();
    private boolean emisAllowMissingCodes;
    private boolean emisAllowUnmappedRegistrationTypes;
    private Set<String> softwareFormatsToDrainQueueOnFailure;
    private boolean transformCerner21Files;
    private int maxTransformErrorsBeforeAbort;
    private List<Pattern> warningsToFailOn = new ArrayList<>();
    private boolean disableSavingResources;
    private boolean validateResourcesOnSaving;
    private int resourceCacheMaxSizeInMemory;
    private String resourceCacheTempPath;

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
        this.emisDisabledOragnisationsAllowed = new ArrayList<>();
        this.emisAllowMissingCodes = false;
        this.emisAllowUnmappedRegistrationTypes = false;
        this.softwareFormatsToDrainQueueOnFailure = new HashSet<>();
        this.transformCerner21Files = false;
        this.maxTransformErrorsBeforeAbort = 50;
        this.warningsToFailOn = new ArrayList<>();
        this.disableSavingResources = false;
        this.validateResourcesOnSaving = true;
        this.resourceCacheMaxSizeInMemory = 100000;
        this.resourceCacheTempPath = null; //using null means we'll offload resources to the DB

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

            node = json.get("emis");
            if (node != null) {

                JsonNode subNode = node.get("disabled_ods_codes_allowed");
                if (subNode != null) {
                    for (int i=0; i<subNode.size(); i++) {
                        String s = subNode.get(i).asText();
                        Pattern pattern = Pattern.compile(s);
                        this.emisDisabledOragnisationsAllowed.add(pattern);
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
                JsonNode subNode = node.get("transform_2_1_files");
                if (subNode != null) {
                    this.transformCerner21Files = subNode.asBoolean();
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

        } catch (Exception var4) {
            //if the config record is there, just log it out rather than throw an exception
            LOG.warn("No common queuereader config found in config DB with app_id queuereader and config_id common_config");
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

    public List<Pattern> getEmisDisabledOragnisationsAllowed() {
        return emisDisabledOragnisationsAllowed;
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

    public boolean isTransformCerner21Files() {
        return transformCerner21Files;
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
}
