package org.endeavourhealth.transform.common;

import org.apache.commons.io.FilenameUtils;

public class ExchangeHelper {

    /**
     * tokenises the exchange body into a list of file names and prepends each element with the shared storage path,
     * so they're ready for access
     */
    public static String[] parseExchangeBodyIntoFileList(String exchangeBody) {
        String sharedStoragePath = TransformConfig.instance().getSharedStoragePath();

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
