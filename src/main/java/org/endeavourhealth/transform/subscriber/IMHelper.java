package org.endeavourhealth.transform.subscriber;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class IMHelper {
    private static final Logger LOG = LoggerFactory.getLogger(IMHelper.class);

    //TODO these 2 are for a quick fix until we can do in IM deploy.
    private final static String URL_ENCODED_LEFT_CURLY = "%7B";
    private final static String URL_ENCODED_RIGHT_CURLY = "%7D";

    //simpler to use just a map in memory than mess about with JCS etc.
    //there won't be so many concepts that we need to worry about limiting in size
    private static Map<String, Integer> coreCache = new ConcurrentHashMap<>();
    private static Vector<String> nullCoreCache = new Vector();
    private static Map<String, Integer> mappedCache = new ConcurrentHashMap<>();
    private static Vector<String> nullMappedCache = new Vector();
    private static Map<Integer, String> snomedCodeCache = new ConcurrentHashMap<>();
    private static Vector<Integer> nullSnomedCodeCache = new Vector();

    public static synchronized Integer getIMMappedConcept(SubscriberTransformHelper params, Resource fhirResource, String scheme, String code) throws Exception {
        String key = createCacheKey(scheme, code);
        Integer ret = null;
        if (code != null) {
            ret = mappedCache.get(key);
        }
        if (ret == null && nullMappedCache.contains(key)) {
            return null;
        }
        if (ret == null & code != null) {
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
        if (ret == null) {
            nullMappedCache.add(key);
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

    public static synchronized Integer getIMConcept(SubscriberTransformHelper params, Resource fhirResource, String scheme, String code, String termIn) throws Exception {
        String key = createCacheKey(scheme, code);
        Integer ret = null;
        if (code != null) {
            ret = coreCache.get(key);
        }
        if (ret == null && nullCoreCache.contains(key)) {
            return null;
        }
        //TODO workaround until we can do an IM deploy. Disruptive
        String term = termIn.replace("{", URL_ENCODED_LEFT_CURLY).replace("}", URL_ENCODED_RIGHT_CURLY); //Jersey treats {} as a parameterized parm
        if (ret == null & code != null) {
            ret = getConceptIdForSchemeCodeWithRetry(scheme, code, term);
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
        if (ret == null) {
            nullCoreCache.add(key);
        }
        return ret;
    }

    private static Integer getConceptIdForSchemeCodeWithRetry(String scheme, String code, String term) throws Exception {

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = 5;

        while (true) {
            lives --;
            try {
                return IMClient.getConceptIdForSchemeCode(scheme, code, true, term);
            } catch (Exception ex) {
                if (lives <= 0) {
                    throw ex;
                }

                LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
            }
        }
    }

    public static synchronized Integer getIMMappedConceptForTypeTerm(SubscriberTransformHelper params, Resource fhirResource, String type, String term) throws Exception {
        String key = createCacheKey(type, term);
        Integer ret = null;
        if (term != null) {
            ret = mappedCache.get(key);
        }
        if (ret == null && nullMappedCache.contains(key)) {
            return null;
        }
        if (ret == null && term != null) {
            ret = getIMMappedConceptForTypeTermWithRetry(type, term);
            if (ret == null) {
                //if null, we may let it slide if in testing, just logging it out
                if (TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                   TransformWarnings.log(LOG, params, "Null mapped IM concept for type {} and term {} for resource {} {}", type, term, fhirResource.getResourceType(), fhirResource.getId());
                } else {
                    throw new TransformException("Null mapped IM concept for type " + type + " and term " + term + " for resource " + fhirResource.getResourceType() + " " + fhirResource.getId());
                }

            } else {
                mappedCache.put(key, ret);
            }
        }
        if (ret == null) {
            nullMappedCache.add(key);
        }
        return ret;
    }

    private static Integer getIMMappedConceptForTypeTermWithRetry(String type, String term) throws Exception {

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = 5;

        while (true) {
            lives --;
            try {
                return IMClient.getMappedCoreConceptIdForTypeTerm(type, term);
            } catch (Exception ex) {
                if (lives <= 0) {
                    throw ex;
                }

                LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
            }
        }
    }

    public static synchronized Integer getIMConceptForTypeTerm(SubscriberTransformHelper params, Resource fhirResource, String type, String term) throws Exception {
        String key = createCacheKey(type, term);
        Integer ret = null;
        if (term != null) {
            ret = coreCache.get(key);
        }
        if (ret == null && nullCoreCache.contains(key)) {
            return null;
        }
        if (ret == null && term != null) {
            ret = getIMConceptForTypeTermWithRetry(type, term);
            if (ret == null) {
                //if null, we may let it slide if in testing, just logging it out
                if (TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                    TransformWarnings.log(LOG, params, "Null IM concept for type {} and term {} for resource {} {}", type, term, fhirResource.getResourceType(), fhirResource.getId());
                } else {
                    throw new TransformException("Null IM concept for type " + type + " and term " + term + " for resource " + fhirResource.getResourceType() + " " + fhirResource.getId());
                }

            } else {
                coreCache.put(key, ret);
            }
        }
        if (ret == null) {
            nullCoreCache.add(key);
        }
        return ret;
    }

    private static Integer getIMConceptForTypeTermWithRetry(String type, String term) throws Exception {

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = 5;

        while (true) {
            lives --;
            try {
                return IMClient.getConceptIdForTypeTerm(type, term, true);
            } catch (Exception ex) {
                if (lives <= 0) {
                    throw ex;
                }
                LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
            }
        }
    }

    public static synchronized String getIMSnomedCodeForConceptId(SubscriberTransformHelper params, Resource fhirResource, Integer conceptId) throws Exception {
        Integer key = conceptId;
        String ret = null;

        if (key != null) {
            ret = snomedCodeCache.get(key);
        }
        if (ret == null && nullSnomedCodeCache.contains(key)) {
            return null;
        }
        if (ret == null && conceptId != null) {
            ret = getIMSnomedCodeForConceptIdWithRetry(conceptId);
            if (ret == null) {
                //if null, we may let it slide if in testing, just logging it out
                if (TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                    TransformWarnings.log(LOG, params, "Null mapped Snomed Code for Concept Id {} for Resource {} {}", conceptId, fhirResource.getResourceType(), fhirResource.getId());
                } else {
                    throw new TransformException("Null mapped Snomed Code for Concept Id " + conceptId + " for Resource " + fhirResource.getResourceType() + " " + fhirResource.getId());
                }

            } else {
                snomedCodeCache.put(key, ret);
            }
        }
        if (ret == null) {
            nullSnomedCodeCache.add(key);
        }
        return ret;
    }

    private static String getIMSnomedCodeForConceptIdWithRetry(Integer conceptId) throws Exception {

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = 5;

        while (true) {
            lives --;
            try {
                return IMClient.getCodeForConceptId(conceptId);
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
