package org.endeavourhealth.transform.ui.models.resources.clinicial;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.endeavourhealth.transform.ui.models.resources.UIResource;
import org.endeavourhealth.transform.ui.models.types.UICodeableConcept;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.endeavourhealth.transform.ui.models.types.UIInternalIdentifier;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UIDiagnosticReport extends UIResource<UIDiagnosticReport> {
    private String status;
    private UIDate effectiveDate;
    private UIInternalIdentifier recordingPractitioner;
    private UIEncounter encounter;
    private UICodeableConcept code;
    private String conclusion;
    private List<UIObservationRelation> related;

    public String getStatus() {
        return status;
    }

    public UIDiagnosticReport setStatus(String status) {
        this.status = status;
        return this;
    }

    public UIInternalIdentifier getRecordingPractitioner() {
        return recordingPractitioner;
    }

    public UIDiagnosticReport setRecordingPractitioner(UIInternalIdentifier recordingPractitioner) {
        this.recordingPractitioner = recordingPractitioner;
        return this;
    }

    public UIEncounter getEncounter() {
        return encounter;
    }

    public UIDiagnosticReport setEncounter(UIEncounter encounter) {
        this.encounter = encounter;
        return this;
    }

    public UICodeableConcept getCode() {
        return code;
    }

    public UIDiagnosticReport setCode(UICodeableConcept code) {
        this.code = code;
        return this;
    }

    public String getConclusion() {
        return conclusion;
    }

    public UIDiagnosticReport setConclusion(String conclusion) {
        this.conclusion = conclusion;
        return this;
    }

    public UIDate getEffectiveDate() {
        return effectiveDate;
    }

    public UIDiagnosticReport setEffectiveDate(UIDate effectiveDate) {
        this.effectiveDate = effectiveDate;
        return this;
    }

    public List<UIObservationRelation> getRelated() {
        return related;
    }

    public UIDiagnosticReport setRelated(List<UIObservationRelation> related) {
        this.related = related;
        return this;
    }
}
