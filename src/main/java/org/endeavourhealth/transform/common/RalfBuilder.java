package org.endeavourhealth.transform.common;

import OpenPseudonymiser.Crypto;
import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.subscriberTransform.PseudoIdDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.PseudoIdAudit;
import org.endeavourhealth.transform.subscriber.json.LinkDistributorConfig;
import java.util.*;

public class RalfBuilder {

    public static final String PATIENT_FIELD_UPRN = "uprn";

    private String uprn;
    private String subscriberConfigName;
    private String saltKeyName;
    private byte[] saltBytes;
    private TreeMap<String, String> treeMap;

    private RalfBuilder(String uprn, String subscriberConfigName, String saltKeyName, String saltBase64) {
        this(uprn, subscriberConfigName, saltKeyName, Base64.getDecoder().decode(saltBase64));
    }

    private RalfBuilder(String uprn, String subscriberConfigName, String saltKeyName, byte[] saltBytes) {
        if (Strings.isNullOrEmpty(uprn)) {
            throw new RuntimeException("Null or empty uprn");
        }
        if (Strings.isNullOrEmpty(subscriberConfigName)) {
            throw new RuntimeException("Null or empty subscriber config name");
        }
        if (Strings.isNullOrEmpty(saltKeyName)) {
            throw new RuntimeException("Null or empty salt key name");
        }
        if (saltBytes == null) {
            throw new RuntimeException("Null salt key bytes");
        }
        this.uprn = uprn;
        this.subscriberConfigName = subscriberConfigName;
        this.saltKeyName = saltKeyName;
        this.saltBytes = saltBytes;
    }

     private boolean addValue(String fieldName, String fieldValue) {

        if (fieldName == null) {
            throw new RuntimeException("Null field name");
        }

        if (Strings.isNullOrEmpty(fieldValue)) {
            return false;
        }

        if (treeMap == null) {
            treeMap = new TreeMap<>();
        }

        treeMap.put(fieldName, fieldValue);
        return true;
    }

    private String createRalf() throws Exception {
        if (treeMap == null
                || treeMap.isEmpty()) {
            return null;
        }

        Crypto crypto = new Crypto();
        crypto.SetEncryptedSalt(saltBytes);
        String ralf = crypto.GetDigest(this.treeMap);

        return ralf;
    }

    private TreeMap<String, String> getKeys() {
        return treeMap;
    }

    public static Map<LinkDistributorConfig, PseudoIdAudit> generateRalfsFromConfigs(String uprn, String subscriberConfigName, List<LinkDistributorConfig> configs) throws Exception {

        Map<LinkDistributorConfig, PseudoIdAudit> ret = new HashMap<>();
        List<PseudoIdAudit> toAudit = new ArrayList<>();

        for (LinkDistributorConfig config: configs) {

            RalfBuilder ralfBuilder = new RalfBuilder(uprn, subscriberConfigName, config.getSaltKeyName(), config.getSalt());
            ralfBuilder.addValue(PATIENT_FIELD_UPRN, uprn);

            String ralf = ralfBuilder.createRalf();
            if (!Strings.isNullOrEmpty(ralf)) {
                PseudoIdAudit audit =  new PseudoIdAudit(config.getSaltKeyName(), ralfBuilder.getKeys(), ralf);
                ret.put(config, audit);
                toAudit.add(audit);
            }
        }

        // audit everything that has been generated
        if (!(toAudit.isEmpty())) {
            PseudoIdDalI pseudoIdDal = DalProvider.factoryPseudoIdDal(subscriberConfigName);
            pseudoIdDal.auditPseudoIds(toAudit);
        }
        return ret;
    }
}