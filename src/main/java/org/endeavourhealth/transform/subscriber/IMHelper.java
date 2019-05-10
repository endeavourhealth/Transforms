package org.endeavourhealth.transform.subscriber;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IMHelper {
    private static final Logger LOG = LoggerFactory.getLogger(IMHelper.class);

    //simpler to use just a map in memory than mess about with JCS etc.
    //there won't be so many concepts that we need to worry about limiting in size
    private static Map<String, Integer> coreCache = new ConcurrentHashMap<>();
    private static Map<String, Integer> mappedCache = new ConcurrentHashMap<>();

    public static Integer getIMMappedConcept(SubscriberTransformParams params, Resource fhirResource, String scheme, String code) throws Exception {
        String key = createCacheKey(scheme, code);
        Integer ret = mappedCache.get(key);
        if (ret == null) {
            ret = getIMMappedConceptWithRetry(scheme, code);
            if (ret == null) {
                //if null, we may let it slide if in testing, just logging it out
                if (TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                    TransformWarnings.log(LOG, params, "Null mapped IM concept for scheme {} and code {} for resource {} {}", scheme, code, fhirResource.getResourceType(), fhirResource.getId());
                } else {
                    throw new TransformException("Null mapped IM concept for scheme " + scheme + " and code " + code + " for resource " + fhirResource.getResourceType() + " " + fhirResource.getId());
                }

            } else {
                mappedCache.put(key, ret);
            }
        }
        return ret;
    }

    private static Integer getIMMappedConceptWithRetry(String scheme, String code) throws Exception {

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = 5;

        while (true) {
            lives --;
            try {
                return IMClient.getMappedCoreConceptIdForSchemeCode(scheme, code);
            } catch (Exception ex) {
                if (lives <= 0) {
                    throw ex;
                }

                LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
            }
        }
    }

    public static Integer getIMConcept(SubscriberTransformParams params, Resource fhirResource, String scheme, String code) throws Exception {
        String key = createCacheKey(scheme, code);
        Integer ret = coreCache.get(key);
        if (ret == null) {
            ret = getConceptIdForSchemeCodeWithRetry(scheme, code);
            if (ret == null) {
                //if null, we may let it slide if in testing, just logging it out
                if (TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                    TransformWarnings.log(LOG, params, "Null IM concept for scheme {} and code {} for resource {} {}", scheme, code, fhirResource.getResourceType(), fhirResource.getId());
                } else {
                    throw new TransformException("Null IM concept for scheme " + scheme + " and code " + code + " for resource " + fhirResource.getResourceType() + " " + fhirResource.getId());
                }

            } else {
                coreCache.put(key, ret);
            }
        }
        return ret;
    }

    private static Integer getConceptIdForSchemeCodeWithRetry(String scheme, String code) throws Exception {

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = 5;

        while (true) {
            lives --;
            try {
                return IMClient.getConceptIdForSchemeCode(scheme, code);
            } catch (Exception ex) {
                if (lives <= 0) {
                    throw ex;
                }

                LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
            }
        }
    }

    private static String createCacheKey(String scheme, String code) {
        return scheme + ":" + code;
    }
}
