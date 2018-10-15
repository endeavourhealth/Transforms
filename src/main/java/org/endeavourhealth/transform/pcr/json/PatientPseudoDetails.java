package org.endeavourhealth.transform.pcr.json;

import java.util.Date;

public class PatientPseudoDetails {
    private String patientId = null;
    private String nhsNumber = null;
    private Date dateOfBirth = null;

    public PatientPseudoDetails() {}

    public PatientPseudoDetails(String patientId, String nhsNumber, Date dateOfBirth) {
        this.patientId = patientId;
        this.nhsNumber = nhsNumber;
        this.dateOfBirth = dateOfBirth;
    }

    public String getPatientId() {
        return patientId;
    }

    public void setPatientId(String patientId) {
        this.patientId = patientId;
    }

    public String getNhsNumber() {
        return nhsNumber;
    }

    public void setNhsNumber(String nhsNumber) {
        this.nhsNumber = nhsNumber;
    }

    public Date getDateOfBirth() {
        return dateOfBirth;
    }

    public void setDateOfBirth(Date dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }
}
