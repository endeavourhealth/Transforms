package org.endeavourhealth.transform.common.exceptions;

import java.io.File;

public class FileFormatException extends TransformException {

    private String filePath = null;

    public FileFormatException(File file, String message) {
        this(file.getAbsolutePath(), message, null);
    }
    public FileFormatException(File file, String message, Throwable cause) {
        this(file.getAbsolutePath(), message, cause);
    }

    public FileFormatException(String filePath, String message) {
        this(filePath, message, null);
    }
    public FileFormatException(String filePath, String message, Throwable cause) {
        super(message, cause);
        this.filePath = filePath;
    }

    public String getFilePath() {
        return filePath;
    }
}
