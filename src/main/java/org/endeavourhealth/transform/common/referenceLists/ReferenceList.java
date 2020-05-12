package org.endeavourhealth.transform.common.referenceLists;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.ParserI;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.List;

/**
 * object to store a list of FHIR References and 0..n CsvCells for each one
 *
 * Made abstract with separate implementations to provide memory optimisations depending
 * on whether we know ahead of time whether we'll be storing 0, 1 or >1 CsvCells for each Reference
 */
public abstract class ReferenceList {

    //changed to arrays to use less memory, since there can be a vast number of these objects in bulk transforms
    //further changed to byte arrays storing the Strings as UTF-8, which is 50% smaller than a String
    //private String[] referencesValues = new String[0];
    private byte[][] referencesValues = new byte[0][];

    public ReferenceList() {}

    public abstract CsvCell[] getSourceCells(int index);
    protected abstract void addSourceCells(CsvCell... csvCells);

    public synchronized final void add(Reference reference, CsvCell... csvCells) {
        String referenceValue = reference.getReference();
        add(referenceValue, csvCells);
    }

    public synchronized final void add(ResourceType resourceType, String id, CsvCell... csvCells) {
        String referenceValue = ReferenceHelper.createResourceReference(resourceType, id);
        add(referenceValue, csvCells);
    }

    /**
     * these may be populated by multiple threads, so synchronise the fn
     */
    private synchronized final void add(String referenceValue, CsvCell... csvCells) {

        //add the reference to our array, encodes as bytes
        byte[] bytes = referenceValue.getBytes(CsvCell.CHARSET);

        int size = size();
        byte[][] newReferenceValues = new byte[size+1][];
        System.arraycopy(referencesValues, 0, newReferenceValues, 0, size);
        newReferenceValues[size] = bytes;
        this.referencesValues = newReferenceValues;

        //replace all CsvCells into our sub-classes that save memory
        for (int i=0; i<csvCells.length; i++) {
            CsvCell csvCell = csvCells[i];
            csvCells[i] = new CsvCellAuditOnly(csvCell);
        }
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

    /**
     * sub-class of CsvCell to retain the audit information from a CsvCell but allow us to de-reference
     * the "value" of the cell. The CsvCells in ReferenceLists are only used for carrying through audit data
     * (i.e. the publishedFileId, recordNumber and colIndex) so we don't need to retain the value in memory.
          */
    static class CsvCellAuditOnly extends CsvCell {

        public CsvCellAuditOnly(CsvCell csvCell) {
            super(csvCell.getPublishedFileId(), csvCell.getRecordNumber(), csvCell.getColIndex(), null, csvCell.getParentParser());
        }


        /**
         * we only retain non-empty cells when adding to this object and the default isEmpty()
         * function tests if the value is present or not, so override to always return true
         */
        @Override
        public boolean isEmpty() {
            return false;
        }

        /**
         * make sure no one tries to treat a CsvCell from a reference list as though it has its value
         */
        @Override
        public String getString() {
            throw new RuntimeException("Should not be trying to access value on CsvCell from ReferenceList");
        }
    }
}
