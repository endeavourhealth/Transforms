package org.endeavourhealth.transform.ui.models.resources.clinicial;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.endeavourhealth.transform.ui.models.resources.UIResource;
import org.endeavourhealth.transform.ui.models.resources.UISimpleEvent;
import org.endeavourhealth.transform.ui.models.types.UICodeableConcept;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.endeavourhealth.transform.ui.models.types.UIInternalIdentifier;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UIDiagnosticOrder extends UIResource<UIDiagnosticOrder> {
    private UIDate effectiveDate;
    private UIInternalIdentifier recordingPractitioner;
    private UIEncounter encounter;
    private UICodeableConcept code;
    private List<UISimpleEvent> events;
    private UISimpleEvent status;

    public UIInternalIdentifier getRecordingPractitioner() {
        return recordingPractitioner;
    }

    public UIDiagnosticOrder setRecordingPractitioner(UIInternalIdentifier recordingPractitioner) {
        this.recordingPractitioner = recordingPractitioner;
        return this;
    }

    public UIEncounter getEncounter() {
        return encounter;
    }

    public UIDiagnosticOrder setEncounter(UIEncounter encounter) {
        this.encounter = encounter;
        return this;
    }

    public UICodeableConcept getCode() {
        return code;
    }

    public UIDiagnosticOrder setCode(UICodeableConcept code) {
        this.code = code;
        return this;
    }

    public UIDate getEffectiveDate() {
        return effectiveDate;
    }

    public UIDiagnosticOrder setEffectiveDate(UIDate effectiveDate) {
        this.effectiveDate = effectiveDate;
        return this;
    }

    public List<UISimpleEvent> getEvents() {
        return events;
    }

    public UIDiagnosticOrder setEvents(List<UISimpleEvent> events) {
        this.events = events;
        return this;
    }

    public UISimpleEvent getStatus() {
        return status;
    }

    public UIDiagnosticOrder setStatus(UISimpleEvent status) {
        this.status = status;
        return this;
    }
}
