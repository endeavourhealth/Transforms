package org.endeavourhealth.transform.common.referenceLists;

import org.endeavourhealth.transform.common.CsvCell;

/**
 * implementation of a ReferenceList that supports multiple associated CsvCell for each reference String, which is
 * less memory efficient than the other implementations and so should only be used when required
 */
public class ReferenceListMultipleCsvCells extends ReferenceList {

    //changed to arrays to use less memory, since there can be a vast number of these objects in bulk transforms
    private CsvCell[][] sourceCells = new CsvCell[0][];

    @Override
    public CsvCell[] getSourceCells(int index) {
        return sourceCells[index];
    }

    @Override
    protected void addSourceCells(CsvCell... csvCells) {
        int size = sourceCells.length;
        CsvCell[][] newSourceCells = new CsvCell[size+1][];
        System.arraycopy(sourceCells, 0, newSourceCells, 0, size);
        newSourceCells[size] = csvCells;
        this.sourceCells = newSourceCells;
    }


}
