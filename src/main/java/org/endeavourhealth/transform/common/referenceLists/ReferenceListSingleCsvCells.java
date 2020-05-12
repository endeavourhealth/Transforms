package org.endeavourhealth.transform.common.referenceLists;

import org.endeavourhealth.transform.common.CsvCell;

/**
 * implementation of a ReferenceList that only supports a single associated CsvCell for each reference String, to save memory
 */
public class ReferenceListSingleCsvCells extends ReferenceList {

    //use an array to use less memory, since there can be a vast number of these objects in bulk transforms
    private CsvCell[] sourceCells = new CsvCell[0]; //array so we have ONE cell per reference in our list

    @Override
    public CsvCell[] getSourceCells(int index) {
        CsvCell ret = sourceCells[index];
        if (ret == null) {
            return new CsvCell[0];
        } else {
            return new CsvCell[]{ret};
        }
    }

    @Override
    protected void addSourceCells(CsvCell... csvCells) {
        if (csvCells.length > 1) {
            throw new RuntimeException("Trying to add " + csvCells.length + " CsvCells to a reference list that only supports one");
        }

        CsvCell cell = null;
        if (csvCells.length > 0) {
            cell = csvCells[0];
        }

        int size = sourceCells.length;
        CsvCell[] newSourceCells = new CsvCell[size+1];
        System.arraycopy(sourceCells, 0, newSourceCells, 0, size);
        newSourceCells[size] = cell;
        this.sourceCells = newSourceCells;
    }

}
