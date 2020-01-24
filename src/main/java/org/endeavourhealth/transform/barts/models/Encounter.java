package org.endeavourhealth.transform.barts.models;

import java.util.Date;

public class Encounter {

    private String encounterId;
    private int patientId;
    private int practitionerId;
    private int appointmentId;
    private Date effectiveDate;
    private Date effectiveEndDate;
    private int episodeOfCareId;
    private String serviceProviderOrganisationId;
    private String encounterType;
    private String parentEncounterId;
    private String additionalFieldsJson;

    public Encounter() {
    }


    public String getEncounterId() {
        return encounterId;
    }

    public void setEncounterId(String encounterId) {
        this.encounterId = encounterId;
    }

    public int getPatientId() {
        return patientId;
    }

    public void setPatientId(int patientId) {
        this.patientId = patientId;
    }

    public int getPractitionerId() {
        return practitionerId;
    }

    public void setPractitionerId(int practitionerId) {
        this.practitionerId = practitionerId;
    }

    public int getAppointmentId() {
        return appointmentId;
    }

    public void setAppointmentId(int appointmentId) {
        this.appointmentId = appointmentId;
    }

    public Date getEffectiveDate() {
        return effectiveDate;
    }

    public void setEffectiveDate(Date effectiveDate) {
        this.effectiveDate = effectiveDate;
    }

    public Date getEffectiveEndDate() {
        return effectiveEndDate;
    }

    public void setEffectiveEndDate(Date effectiveEndDate) {
        this.effectiveEndDate = effectiveEndDate;
    }

    public int getEpisodeOfCareId() {
        return episodeOfCareId;
    }

    public void setEpisodeOfCareId(int episodeOfCareId) {
        this.episodeOfCareId = episodeOfCareId;
    }

    public String getServiceProviderOrganisationId() {
        return serviceProviderOrganisationId;
    }

    public void setServiceProviderOrganisationId(String serviceProviderOrganisationId) {
        this.serviceProviderOrganisationId = serviceProviderOrganisationId;
    }

    public String getEncounterType() {
        return encounterType;
    }

    public void setEncounterType(String encounterType) {
        this.encounterType = encounterType;
    }

    public String getParentEncounterId() {
        return parentEncounterId;
    }

    public void setParentEncounterId(String parentEncounterId) {
        this.parentEncounterId = parentEncounterId;
    }

    public String getAdditionalFieldsJson() {
        return additionalFieldsJson;
    }

    public void setAdditionalFieldsJson(String additionalFieldsJson) {
        this.additionalFieldsJson = additionalFieldsJson;
    }
}