package org.endeavourhealth.transform.emis.csv;

import org.apache.commons.io.FilenameUtils;

public class CsvCurrentState {

    private String fileName;
    //private String fileDir;
    private Long recordNumber;

    public CsvCurrentState(String filePath, long recordNumber) {
        this.fileName = FilenameUtils.getName(filePath);
        //this.fileDir = file.getParent();
        this.recordNumber = Long.valueOf(recordNumber);
    }

    public String getFileName() {
        return fileName;
    }

    /*public String getFileDir() {
        return fileDir;
    }*/

    public Long getRecordNumber() {
        return recordNumber;
    }

    public String toString() {
        String ret = fileName;
        if (recordNumber != null) {
            ret += " line: " + recordNumber;
        }
        return ret;
    }
    /*public String toString() {
        String ret = fileDir + "/" + fileName;
        if (recordNumber != null) {
            ret += " line: " + recordNumber;
        }
        return ret;
    }*/
}
