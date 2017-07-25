package org.endeavourhealth.transform.ui.models.resources.clinicial;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.endeavourhealth.transform.ui.models.resources.UIResource;
import org.endeavourhealth.transform.ui.models.resources.UISimpleEvent;
import org.endeavourhealth.transform.ui.models.types.UICodeableConcept;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.endeavourhealth.transform.ui.models.types.UIInternalIdentifier;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class UISpecimen extends UIResource<UISpecimen> {
    private UIDate effectiveDate;
    private UIInternalIdentifier recordingPractitioner;
    private UICodeableConcept code;
    private UISimpleEvent status;

    public UIInternalIdentifier getRecordingPractitioner() {
        return recordingPractitioner;
    }

    public UISpecimen setRecordingPractitioner(UIInternalIdentifier recordingPractitioner) {
        this.recordingPractitioner = recordingPractitioner;
        return this;
    }

    public UICodeableConcept getCode() {
        return code;
    }

    public UISpecimen setCode(UICodeableConcept code) {
        this.code = code;
        return this;
    }

    public UIDate getEffectiveDate() {
        return effectiveDate;
    }

    public UISpecimen setEffectiveDate(UIDate effectiveDate) {
        this.effectiveDate = effectiveDate;
        return this;
    }

    public UISimpleEvent getStatus() {
        return status;
    }

    public UISpecimen setStatus(UISimpleEvent status) {
        this.status = status;
        return this;
    }
}
