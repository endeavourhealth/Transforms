package org.endeavourhealth.transform.emis.csv.helpers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.Reference;

import java.util.List;

public class ReferenceList {

    //changed to arrays to use less memory, since there can be a vast number of these objects in bulk transforms
    private String[] referencesValues = new String[0];
    private CsvCell[][] sourceCells = new CsvCell[0][];
    /*private List<String> referencesValuesList = new ArrayList<>(); //store references as Strings to reduce memory
    private List<CsvCell[]> sourceCellsList = new ArrayList<>();*/

    public ReferenceList() {}

    /**
     * these may be populated by multiple threads, so synchronise the fn
     */
    public synchronized void add(Reference reference, CsvCell... sourceCells) {
        String referenceValue = reference.getReference();

        int size = size();
        String[] newReferenceValues = new String[size+1];
        CsvCell[][] newSourceCells = new CsvCell[size+1][];
        System.arraycopy(referencesValues, 0, newReferenceValues, 0, size);
        System.arraycopy(sourceCells, 0, newSourceCells, 0, size);

        newReferenceValues[size] = referenceValue;
        newSourceCells[size] = sourceCells;

        /*referencesValuesList.add(referenceValue);
        sourceCellsList.add(sourceCells);*/
    }

    public void add(List<Reference> references) {
        for (Reference reference: references) {
            add(reference);
        }
    }

    public int size() {
        return referencesValues.length;
        //return referencesValuesList.size();
    }

    public String getReferenceValue(int index) {
        return referencesValues[index];
        //return referencesValuesList.get(index);
    }

    public Reference getReference(int index) {
        return ReferenceHelper.createReference(getReferenceValue(index));
    }

    public CsvCell[] getSourceCells(int index) {
        return sourceCells[index];
        //return sourceCellsList.get(index);
    }
}
