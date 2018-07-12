package org.endeavourhealth.transform.common;

import com.google.gson.JsonSyntaxException;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.JsonSerializer;

import java.util.ArrayList;
import java.util.List;

public class ExchangeHelper {

    public static List<ExchangePayloadFile> parseExchangeBody(String exchangeBody) {
        String sharedStoragePath = TransformConfig.instance().getSharedStoragePath();

        List<ExchangePayloadFile> ret = new ArrayList<>();

        ExchangePayloadFile[] files = JsonSerializer.deserialize(exchangeBody, ExchangePayloadFile[].class);

        for (int i=0; i<files.length; i++) {
            ExchangePayloadFile file = files[i];
            String path = FilenameUtils.concat(sharedStoragePath, file.getPath());
            file.setPath(path);
            ret.add(file);
        }

        return ret;
    }

    /**
     * tokenises the exchange body into a list of file names and prepends each element with the shared storage path,
     * so they're ready for access
     */
    public static String[] parseExchangeBodyOldWay(String exchangeBody) {
        String sharedStoragePath = TransformConfig.instance().getSharedStoragePath();

        try {
            ExchangePayloadFile[] files = JsonSerializer.deserialize(exchangeBody, ExchangePayloadFile[].class);

            String[] ret = new String[files.length];
            for (int i=0; i<files.length; i++) {
                ExchangePayloadFile file = files[i];
                String path = FilenameUtils.concat(sharedStoragePath, file.getPath());
                file.setPath(path);
                ret[i] = file.getPath();
            }
            return ret;

        } catch (JsonSyntaxException ex) {
            //if the JSON can't be parsed, then it'll be the old format of body that isn't JSON

            //split by /n but trim each one, in case there's a sneaky /r in there
            String[] files = exchangeBody.split("\n");
            for (int i=0; i<files.length; i++) {
                String file = files[i].trim();
                String filePath = FilenameUtils.concat(sharedStoragePath, file);
                files[i] = filePath;
            }

            return files;
        }
    }

    /*public static String[] parseExchangeBodyOldWay(String exchangeBody) {
        String sharedStoragePath = TransformConfig.instance().getSharedStoragePath();

        //split by /n but trim each one, in case there's a sneaky /r in there
        String[] files = exchangeBody.split("\n");
        for (int i=0; i<files.length; i++) {
            String file = files[i].trim();
            String filePath = FilenameUtils.concat(sharedStoragePath, file);
            files[i] = filePath;
        }

        return files;
    }*/
}
