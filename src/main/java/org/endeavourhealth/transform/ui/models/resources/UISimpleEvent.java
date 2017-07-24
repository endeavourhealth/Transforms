package org.endeavourhealth.transform.ui.models.resources;

import org.endeavourhealth.transform.ui.models.types.UIDate;

public class UISimpleEvent {
    private String status;
    private UIDate date;

    public String getStatus() {
        return status;
    }

    public UISimpleEvent setStatus(String status) {
        this.status = status;
        return this;
    }

    public UIDate getDate() {
        return date;
    }

    public UISimpleEvent setDate(UIDate date) {
        this.date = date;
        return this;
    }
}
