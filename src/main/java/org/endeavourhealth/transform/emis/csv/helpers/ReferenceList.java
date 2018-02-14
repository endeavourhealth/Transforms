package org.endeavourhealth.transform.emis.csv.helpers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.Reference;

import java.util.ArrayList;
import java.util.List;

public class ReferenceList {

    private List<String> referencesValuesList = new ArrayList<>(); //store references as Strings to reduce memory
    private List<CsvCell[]> sourceCellsList = new ArrayList<>();

    public ReferenceList() {}

    /**
     * these may be populated by multiple threads, so synchronise the fn
     */
    public synchronized void add(Reference reference, CsvCell... sourceCells) {
        String referenceValue = reference.getReference();

        referencesValuesList.add(referenceValue);
        sourceCellsList.add(sourceCells);
    }

    public void add(List<Reference> references) {
        for (Reference reference: references) {
            add(reference);
        }
    }

    public int size() {
        return referencesValuesList.size();
    }

    public String getReferenceValue(int index) {
        return referencesValuesList.get(index);
    }

    public Reference getReference(int index) {
        return ReferenceHelper.createReference(getReferenceValue(index));
    }

    public CsvCell[] getSourceCells(int index) {
        return sourceCellsList.get(index);
    }
}
