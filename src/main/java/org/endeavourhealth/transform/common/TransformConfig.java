package org.endeavourhealth.transform.common;

import com.fasterxml.jackson.databind.JsonNode;
import org.endeavourhealth.common.config.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class TransformConfig {
    private static final Logger LOG = LoggerFactory.getLogger(TransformConfig.class);

    private String sharedStoragePath;
    private int attemptsPermmitedPerExchange;
    private String killFileLocation;
    private boolean emisAllowDisabledOrganisations;
    private boolean emisAllowMissingCodes;
    private Set<String> softwareFormatsToDrainQueueOnFailure;
    private boolean transformCerner21Files;

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
        this.emisAllowDisabledOrganisations = false;
        this.emisAllowMissingCodes = false;
        this.softwareFormatsToDrainQueueOnFailure = new HashSet<>();
        this.transformCerner21Files = false;

        try {
            JsonNode json = ConfigManager.getConfigurationAsJson("common_config", "queuereader");

            JsonNode node = json.get("shared_storage_path");
            if (node != null) {
                this.sharedStoragePath = node.asText();
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

            node = json.get("emis");
            if (node != null) {
                JsonNode subNode = node.get("process_disabled_services");
                if (subNode != null) {
                    this.emisAllowDisabledOrganisations = subNode.asBoolean();
                }

                subNode = node.get("allow_missing_codes");
                if (subNode != null) {
                    this.emisAllowMissingCodes = subNode.asBoolean();
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

    public boolean isEmisAllowDisabledOrganisations() {
        return emisAllowDisabledOrganisations;
    }

    public boolean isEmisAllowMissingCodes() {
        return emisAllowMissingCodes;
    }

    public Set<String> getSoftwareFormatsToDrainQueueOnFailure() {
        return softwareFormatsToDrainQueueOnFailure;
    }

    public boolean isTransformCerner21Files() {
        return transformCerner21Files;
    }
}
