package org.endeavourhealth.transform.subscriber;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IMHelper {
    private static final Logger LOG = LoggerFactory.getLogger(IMHelper.class);

    //simpler to use just a map in memory than mess about with JCS etc.
    private static Map<String, Integer> coreCache = new ConcurrentHashMap<>();
    private static Map<String, Integer> mappedCache = new ConcurrentHashMap<>();

    public static Integer getIMMappedConcept(SubscriberTransformParams params, String scheme, String code) throws Exception {
        String key = createCacheKey(scheme, code);
        Integer ret = mappedCache.get(key);
        if (ret == null) {
            ret = IMClient.getMappedCoreConceptIdForSchemeCode(scheme, code);
            if (ret == null) {
                //if null, we may let it slide if in testing, just logging it out
                if (TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                    TransformWarnings.log(LOG, params, "Null mapped IM concept for scheme {} and code {}", scheme, code);
                } else {
                    throw new TransformException("Null mapped IM concept for scheme " + scheme + " and code " + code);
                }

            } else {
                mappedCache.put(key, ret);
            }
        }
        return ret;
    }

    public static Integer getIMConcept(SubscriberTransformParams params, String scheme, String code) throws Exception {
        String key = createCacheKey(scheme, code);
        Integer ret = coreCache.get(key);
        if (ret == null) {
            ret = IMClient.getConceptIdForSchemeCode(scheme, code);
            if (ret == null) {
                //if null, we may let it slide if in testing, just logging it out
                if (TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                    TransformWarnings.log(LOG, params, "Null IM concept for scheme {} and code {}", scheme, code);
                } else {
                    throw new TransformException("Null IM concept for scheme " + scheme + " and code " + code);
                }

            } else {
                coreCache.put(key, ret);
            }
        }
        return ret;
    }

    private static String createCacheKey(String scheme, String code) {
        return scheme + ":" + code;
    }
}
