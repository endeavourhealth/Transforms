package org.endeavourhealth.transform.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

public class ExchangePayloadFile {

    private String path = null;
    private Long size = null;
    private String type = null;

    public ExchangePayloadFile() {}

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }


    public static String validateFilesAreInSameDirectory(List<ExchangePayloadFile> files) throws FileNotFoundException {
        String ret = null;

        for (ExchangePayloadFile file: files) {

            File f = new File(file.getPath()); //this still works even for an S3 path
            String parent = f.getParent();

            if (ret == null) {
                ret = parent;

            } else {
                if (!ret.equalsIgnoreCase(parent)) {
                    String err = "" + f + " isn't in the expected directory structure within " + ret;
                    for (ExchangePayloadFile s: files) {
                        err += "\n";
                        err += s.getPath();
                    }
                    throw new FileNotFoundException(err);
                }
            }
        }

        return ret;
    }

}
