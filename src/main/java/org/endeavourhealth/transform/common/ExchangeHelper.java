package org.endeavourhealth.transform.common;

import com.google.gson.JsonSyntaxException;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.JsonSerializer;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.database.dal.audit.models.Exchange;
import org.endeavourhealth.core.database.dal.audit.models.HeaderKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class ExchangeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(ExchangeHelper.class);

    public static final UUID DUMMY_BATCH_ID = UUID.fromString("00000000-0000-0000-0000-000000000000"); //used as a non-null batch ID when we don't have one

    public static List<ExchangePayloadFile> parseExchangeBody(String exchangeBody) {
        return parseExchangeBody(exchangeBody, true);
    }

    public static List<ExchangePayloadFile> parseExchangeBody(String exchangeBody, boolean addSharedStoragePath) {
        String sharedStoragePath = TransformConfig.instance().getSharedStoragePath();

        List<ExchangePayloadFile> ret = new ArrayList<>();

        ExchangePayloadFile[] files = JsonSerializer.deserialize(exchangeBody, ExchangePayloadFile[].class);

        for (int i = 0; i < files.length; i++) {
            ExchangePayloadFile file = files[i];
            if (addSharedStoragePath) {
                String path = FilenameUtils.concat(sharedStoragePath, file.getPath());
                file.setPath(path);
            }
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

    public static void filterFileTypes(List<ExchangePayloadFile> files, Service service, UUID exchangeId) throws Exception {
        String odsCode = service.getLocalId();

        Set<String> filteredTypes = TransformConfig.instance().getFilteredFileTypes(odsCode);
        if (filteredTypes == null || filteredTypes.isEmpty()) {
            return;
        }

        LOG.info("Will only process " + filteredTypes.size() + " file types: " + String.join(", ", filteredTypes));

        for (int i=files.size()-1; i>=0; i--) {
            ExchangePayloadFile file = files.get(i);
            if (!filteredTypes.contains(file.getType())) {
                files.remove(i);
            }
        }

        //write to exchange_event so we can see this happened
        AuditWriter.writeExchangeEvent(exchangeId, "Automatically filtering on file types (" + String.join(", ", filteredTypes) + ") leaving " + files.size() + " files");
    }

    /**
     * checks if this exchange has the "last message" header key
     */
    public static boolean isLastMessage(Exchange exchange) {
        Boolean b = exchange.getHeaderAsBoolean(HeaderKeys.LastMessage);
        return b != null && b.booleanValue();
    }

    /**
     * checks if an exchange has been flagged to not allow re-queueing
     */
    public static boolean isAllowRequeueing(Exchange exchange) {
        Boolean b = exchange.getHeaderAsBoolean(HeaderKeys.AllowQueueing);
        return b == null || b.booleanValue();
    }
}
