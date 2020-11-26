package org.endeavourhealth.transform.subscriber.targetTables;

public enum SubscriberTableId {

    PATIENT((byte)2, "patient"),
    PERSON((byte)3, "person"),
    ALLERGY_INTOLERANCE((byte)4, "allergy_intolerance"),
    ENCOUNTER((byte)5, "encounter"),
    EPISODE_OF_CARE((byte)6, "episode_of_care"),
    FLAG((byte)7, "flag"),
    LOCATION((byte)8, "location"),
    MEDICATION_ORDER((byte)9, "medication_order"),
    MEDICATION_STATEMENT((byte)10, "medication_statement"),
    OBSERVATION((byte)11, "observation"),
    ORGANIZATION((byte)12, "organization"),
    PRACTITIONER((byte)13, "practitioner"),
    PROCEDURE_REQUEST((byte)14, "procedure_request"),
    //PSEUDO_ID((byte)15, "pseudo_id"), //not used any more - superseded by PATIENT_PSEUDO_ID
    REFERRAL_REQUEST((byte)16, "referral_request"),
    SCHEDULE((byte)17, "schedule"),
    APPOINTMENT((byte)18, "appointment"),
    PATIENT_CONTACT((byte)19, "patient_contact"),
    PATIENT_ADDRESS((byte)20, "patient_address"),
    DIAGNOSTIC_ORDER((byte)21, "diagnostic_order"),
    PATIENT_ADDRESS_MATCH((byte)22, "patient_address_match"),
    REGISTRATION_STATUS_HISTORY((byte)23, "registration_status_history"),
    ORGANIZATION_METADATA((byte)24, "organization_metadata"),
    ENCOUNTER_EVENT((byte)25, "encounter_event"),
    ENCOUNTER_ADDITIONAL((byte)26, "encounter_additional"),
    PATIENT_PSEUDO_ID((byte)27, "patient_pseudo_id"),
    PATIENT_ADDITIONAL((byte)28, "patient_additional"),
    OBSERVATION_ADDITIONAL((byte)29, "observation_additional"),
    PATIENT_ADDRESS_RALF((byte)30, "patient_address_ralf"),
    ORGANIZATION_ADDITIONAL((byte)31, "organization_additional"),
    PROPERTY_V2 ((byte)32, "property_v2"),
    ORGANIZATION_V2((byte)33, "organization_v2"),
    ORGANIZATION_CONTACT_V2((byte)34, "organization_contact_v2"),
    LOCATION_V2((byte)35, "location_v2"),
    ABP_ADDRESS_V2((byte)36, "abp_address_v2"),
    ADDRESS_V2((byte)37, "address_v2"),
    UPRN_MATCH_EVENT_V2((byte)38, "uprn_match_event_v2");

    private byte id;
    private String name;

    public byte getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    SubscriberTableId(byte id, String name) {
        this.id = id;
        this.name = name;
    }

}
