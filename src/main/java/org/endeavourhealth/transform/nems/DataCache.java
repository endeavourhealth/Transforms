package org.endeavourhealth.transform.nems;

import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class DataCache {

    public static HashMap<String, String> organisationMap = new HashMap<>();
    public static List<String> practitioners = new ArrayList<>();
    public static HashMap<String, PatientBuilder> linkRelatedPersonIdToMother = new HashMap<>();
    public static HashMap<String, String> linkedBabyAndMotherIds = new HashMap<>();
    public static HashMap<String, EncounterBuilder> encounterMap = new HashMap<>();
    public static HashMap<String, PatientBuilder> patientMap = new HashMap<>();
    public static List<EncounterBuilder> encounters = new ArrayList<>();
    public static List<EpisodeOfCareBuilder> episodes = new ArrayList<>();
    public static List<ObservationBuilder> observations = new ArrayList<>();
    public static List<PatientBuilder> patientArray = new ArrayList<>();
    public static Date birthDate = null;
    public static final String NEMS_ODS = "X26";


    public static void clearCache(){
        encounters = new ArrayList<>();
        practitioners = new ArrayList<>();
        episodes = new ArrayList<>();
        observations = new ArrayList<>();
        patientArray = new ArrayList<>();
        organisationMap = new HashMap<>();
        encounterMap = new HashMap<>();
        linkedBabyAndMotherIds = new HashMap<>();
        linkRelatedPersonIdToMother = new HashMap<>();
        patientMap = new HashMap<>();
        birthDate = null;
    }

}
