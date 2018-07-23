package org.endeavourhealth.transform.common.referenceLists;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.Reference;

import java.util.List;

public abstract class ReferenceList {

    //changed to arrays to use less memory, since there can be a vast number of these objects in bulk transforms
    //further changed to byte arrays storing the Strings as UTF-8, which is 50% smaller than a String
    //private String[] referencesValues = new String[0];
    private byte[][] referencesValues = new byte[0][];

    public ReferenceList() {}

    public abstract CsvCell[] getSourceCells(int index);
    protected abstract void addSourceCells(CsvCell... csvCells);

    /**
     * these may be populated by multiple threads, so synchronise the fn
     */
    public synchronized final void add(Reference reference, CsvCell... csvCells) {
        String referenceValue = reference.getReference();
        byte[] bytes = referenceValue.getBytes(CsvCell.CHARSET);

        int size = size();
        byte[][] newReferenceValues = new byte[size+1][];
        System.arraycopy(referencesValues, 0, newReferenceValues, 0, size);
        newReferenceValues[size] = bytes;
        this.referencesValues = newReferenceValues;

        /*int size = size();
        String[] newReferenceValues = new String[size+1];
        System.arraycopy(referencesValues, 0, newReferenceValues, 0, size);
        newReferenceValues[size] = referenceValue;
        this.referencesValues = newReferenceValues;*/

        addSourceCells(csvCells);
    }

    public void add(List<Reference> references) {
        for (Reference reference: references) {
            add(reference);
        }
    }

    public int size() {
        return referencesValues.length;
    }

    public String getReferenceValue(int index) {
        byte[] bytes = referencesValues[index];
        return new String(bytes, CsvCell.CHARSET);
        //return referencesValues[index];
    }

    public Reference getReference(int index) {
        return ReferenceHelper.createReference(getReferenceValue(index));
    }

}
