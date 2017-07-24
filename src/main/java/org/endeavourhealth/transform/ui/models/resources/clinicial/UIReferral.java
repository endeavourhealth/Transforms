package org.endeavourhealth.transform.ui.models.resources.clinicial;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.endeavourhealth.transform.ui.models.resources.UIResource;
import org.endeavourhealth.transform.ui.models.types.UICodeableConcept;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.endeavourhealth.transform.ui.models.types.UIInternalIdentifier;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UIReferral extends UIResource<UIReferral> {
    private UIDate effectiveDate;
    private UIInternalIdentifier recordingPractitioner;
    private UICodeableConcept code;

    public UIInternalIdentifier getRecordingPractitioner() {
        return recordingPractitioner;
    }

    public UIReferral setRecordingPractitioner(UIInternalIdentifier recordingPractitioner) {
        this.recordingPractitioner = recordingPractitioner;
        return this;
    }

    public UICodeableConcept getCode() {
        return code;
    }

    public UIReferral setCode(UICodeableConcept code) {
        this.code = code;
        return this;
    }

    public UIDate getEffectiveDate() {
        return effectiveDate;
    }

    public UIReferral setEffectiveDate(UIDate effectiveDate) {
        this.effectiveDate = effectiveDate;
        return this;
    }
}
