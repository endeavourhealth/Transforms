package org.endeavourhealth.transform.common.referenceLists;

import org.endeavourhealth.transform.common.CsvCell;

/**
 * implementation of a ReferenceList that doesn't support associated CsvCells for each reference String, to save memory
 */
public class ReferenceListNoCsvCells extends ReferenceList {

    @Override
    public CsvCell[] getSourceCells(int index) {
        return new CsvCell[0];
    }

    @Override
    protected void addSourceCells(CsvCell... csvCells) {
        if (csvCells.length > 0) {
            throw new RuntimeException("Trying to add " + csvCells.length + " to a ReferenceList that doesn't support CsvCells");
        }
    }
}
