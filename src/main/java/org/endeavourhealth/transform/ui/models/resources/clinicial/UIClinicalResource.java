package org.endeavourhealth.transform.ui.models.resources.clinicial;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.endeavourhealth.transform.ui.models.resources.UIResource;
import org.endeavourhealth.transform.ui.models.types.UICodeableConcept;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.endeavourhealth.transform.ui.models.types.UIInternalIdentifier;

@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class UIClinicalResource<T extends UIClinicalResource> extends UIResource<T> {
    private UICodeableConcept code;
    private UIInternalIdentifier effectivePractitioner;
    private UIDate effectiveDate;
    private UIInternalIdentifier recordingPractitioner;
    private UIDate recordedDate;
    private String notes;

    public UICodeableConcept getCode() {
        return code;
    }

    @SuppressWarnings("unchecked")
    public T setCode(UICodeableConcept code) {
        this.code = code;
        return (T)this;
    }

    public UIInternalIdentifier getEffectivePractitioner() {
        return effectivePractitioner;
    }

    @SuppressWarnings("unchecked")
    public T setEffectivePractitioner(UIInternalIdentifier effectivePractitioner) {
        this.effectivePractitioner = effectivePractitioner;
        return (T)this;
    }

    public UIDate getEffectiveDate() {
        return effectiveDate;
    }

    @SuppressWarnings("unchecked")
    public T setEffectiveDate(UIDate effectiveDate) {
        this.effectiveDate = effectiveDate;
        return (T)this;
    }

    public UIInternalIdentifier getRecordingPractitioner() {
        return recordingPractitioner;
    }

    @SuppressWarnings("unchecked")
    public T setRecordingPractitioner(UIInternalIdentifier recordingPractitioner) {
        this.recordingPractitioner = recordingPractitioner;
        return (T)this;
    }

    public UIDate getRecordedDate() {
        return recordedDate;
    }

    @SuppressWarnings("unchecked")
    public T setRecordedDate(UIDate recordedDate) {
        this.recordedDate = recordedDate;
        return (T)this;
    }

    public String getNotes() {
        return notes;
    }

    @SuppressWarnings("unchecked")
    public T setNotes(String notes) {
        this.notes = notes;
        return (T)this;
    }
}
