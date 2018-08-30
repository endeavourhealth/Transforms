package org.endeavourhealth.transform.emis.csv.helpers;

import org.endeavourhealth.transform.common.CsvCell;

import java.util.ArrayList;
import java.util.List;

public class SessionPractitioners {
    private List<CsvCell> emisUserGuidsToSave = new ArrayList<>();
    private List<CsvCell> emisUserGuidsToDelete = new ArrayList<>();

    public List<CsvCell> getEmisUserGuidsToSave() {
        return emisUserGuidsToSave;
    }

    public void setEmisUserGuidsToSave(List<CsvCell> emisUserGuidsToSave) {
        this.emisUserGuidsToSave = emisUserGuidsToSave;
    }

    public List<CsvCell> getEmisUserGuidsToDelete() {
        return emisUserGuidsToDelete;
    }

    public void setEmisUserGuidsToDelete(List<CsvCell> emisUserGuidsToDelete) {
        this.emisUserGuidsToDelete = emisUserGuidsToDelete;
    }

}
