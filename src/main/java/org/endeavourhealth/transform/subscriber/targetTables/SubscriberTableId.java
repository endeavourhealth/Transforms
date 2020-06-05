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
    PSEUDO_ID((byte)15, "pseudo_id"),
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
    ENCOUNTER_ADDITIONAL((byte)26, "encounter_additional");

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
