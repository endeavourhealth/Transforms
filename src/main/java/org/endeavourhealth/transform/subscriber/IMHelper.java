package org.endeavourhealth.transform.subscriber;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.common.HasServiceSystemAndExchangeIdI;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class IMHelper {
    private static final Logger LOG = LoggerFactory.getLogger(IMHelper.class);

    //simpler to use just a map in memory than mess about with JCS etc.
    //there won't be so many concepts that we need to worry about limiting in size
    private static Map<String, Integer> mappedCache = new ConcurrentHashMap<>();
    private static Set<String> nullMappedCache = ConcurrentHashMap.newKeySet();

    private static Map<String, Integer> coreCache = new ConcurrentHashMap<>();
    private static Set<String> nullCoreCache = ConcurrentHashMap.newKeySet();

    private static Map<Integer, String> snomedCodeCache = new ConcurrentHashMap<>();
    private static Set<Integer> nullSnomedCodeCache = ConcurrentHashMap.newKeySet();

    private static final ReentrantLock lock = new ReentrantLock();

    public static Integer getIMMappedConcept(HasServiceSystemAndExchangeIdI params, Resource fhirResource, String scheme, String code) throws Exception {

        if (code == null) {
            return null;
        }

        //check the cache first
        String key = createCacheKey(scheme, code);
        Integer ret = mappedCache.get(key);
        if (ret != null
                || nullMappedCache.contains(key)) {
            return ret;
        }

        //hit the IM API
        ret = getIMMappedConceptWithRetry(scheme, code);

        //store in the cache
        if (ret == null) {
            nullMappedCache.add(key);

            //if null, we may let it slide if in testing, just logging it out
            if (!TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                throw new TransformException("Null mapped IM concept for scheme " + scheme + " and code " + code + " for resource " + fhirResource.getResourceType() + " " + fhirResource.getId());
            }

        } else {
            mappedCache.put(key, ret);
        }

        return ret;
    }

    private static Integer getIMMappedConceptWithRetry(String scheme, String code) throws Exception {

        try {
            lock.lock();

            //during development, we get fairly frequent timeouts, so give it a couple of attempts
            int lives = 5;

            while (true) {
                lives--;
                try {
                    return IMClient.getMappedCoreConceptIdForSchemeCode(scheme, code);
                } catch (Exception ex) {
                    if (lives <= 0) {
                        throw ex;
                    }

                    LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public static Integer getIMConcept(HasServiceSystemAndExchangeIdI params, Resource fhirResource, String scheme, String code, String term) throws Exception {

        if (code == null) {
            return null;
        }

        //check cache first
        String key = createCacheKey(scheme, code);
        Integer ret = coreCache.get(key);
        if (ret != null
                || nullCoreCache.contains(key)) {
            return ret;
        }

        //hit the IM API
        ret = getConceptIdForSchemeCodeWithRetry(scheme, code, term);

        if (ret == null) {
            nullCoreCache.add(key);

            if (!TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                throw new TransformException("Null IM concept for scheme " + scheme + " and code " + code + " for resource " + fhirResource.getResourceType() + " " + fhirResource.getId());
            }

        } else {
            coreCache.put(key, ret);
        }

        return ret;
    }

    private static Integer getConceptIdForSchemeCodeWithRetry(String scheme, String code, String term) throws Exception {

        try {
            lock.lock();

            //during development, we get fairly frequent timeouts, so give it a couple of attempts
            int lives = 5;

            while (true) {
                lives--;
                try {
                    return IMClient.getConceptIdForSchemeCode(scheme, code, true, term);
                } catch (Exception ex) {
                    if (lives <= 0) {
                        throw ex;
                    }

                    LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
                }
            }

        } finally {
            lock.unlock();
        }
    }

    public static Integer getIMMappedConceptForTypeTerm(HasServiceSystemAndExchangeIdI params, Resource fhirResource, String type, String term) throws Exception {

        if (term == null) {
            return null;
        }

        //check cache
        String key = createCacheKey(type, term);
        Integer ret = mappedCache.get(key);
        if (ret != null
                || nullMappedCache.contains(key)) {
            return ret;
        }

        //hit IM API
        ret = getIMMappedConceptForTypeTermWithRetry(type, term);

        //add to cache
        if (ret == null) {
            nullMappedCache.add(key);

            if (!TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                throw new TransformException("Null mapped IM concept for type " + type + " and term " + term + " for resource " + fhirResource.getResourceType() + " " + fhirResource.getId());
            }

        } else {
            mappedCache.put(key, ret);
        }

        return ret;
    }

    private static Integer getIMMappedConceptForTypeTermWithRetry(String type, String term) throws Exception {

        try {
            lock.lock();

            //during development, we get fairly frequent timeouts, so give it a couple of attempts
            int lives = 5;

            while (true) {
                lives--;
                try {
                    return IMClient.getMappedCoreConceptIdForTypeTerm(type, term);
                } catch (Exception ex) {
                    if (lives <= 0) {
                        throw ex;
                    }

                    LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public static Integer getIMConceptForTypeTerm(HasServiceSystemAndExchangeIdI params, Resource fhirResource, String type, String term) throws Exception {

        if (term == null) {
            return null;
        }

        //check cache
        String key = createCacheKey(type, term);
        Integer ret = coreCache.get(key);
        if (ret != null
                || nullCoreCache.contains(key)) {
            return ret;
        }

        //hit IM API
        ret = getIMConceptForTypeTermWithRetry(type, term);

        //add to cache
        if (ret == null) {
            nullCoreCache.add(key);

            if (!TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                throw new TransformException("Null IM concept for type " + type + " and term " + term + " for resource " + fhirResource.getResourceType() + " " + fhirResource.getId());
            }

        } else {
            coreCache.put(key, ret);
        }

        return ret;
    }

    private static Integer getIMConceptForTypeTermWithRetry(String type, String term) throws Exception {

        try {
            lock.lock();

            //during development, we get fairly frequent timeouts, so give it a couple of attempts
            int lives = 5;

            while (true) {
                lives--;
                try {
                    return IMClient.getConceptIdForTypeTerm(type, term, true);
                } catch (Exception ex) {
                    if (lives <= 0) {
                        throw ex;
                    }
                    LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public static String getIMSnomedCodeForConceptId(HasServiceSystemAndExchangeIdI params, Resource fhirResource, Integer conceptId) throws Exception {

        if (conceptId == null) {
            return null;
        }

        //check cache
        Integer key = conceptId;
        String ret = snomedCodeCache.get(key);
        if (ret != null || nullSnomedCodeCache.contains(key)) {
            return ret;
        }

        //hit IM API
        ret = getIMSnomedCodeForConceptIdWithRetry(conceptId);

        //add to cache
        if (ret == null) {
            nullSnomedCodeCache.add(key);

            if (!TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                throw new TransformException("Null mapped Snomed Code for Concept Id " + conceptId + " for Resource " + fhirResource.getResourceType() + " " + fhirResource.getId());
            }

        } else {
            snomedCodeCache.put(key, ret);
        }

        return ret;
    }

    private static String getIMSnomedCodeForConceptIdWithRetry(Integer conceptId) throws Exception {

        try {
            lock.lock();

            //during development, we get fairly frequent timeouts, so give it a couple of attempts
            int lives = 5;

            while (true) {
                lives--;
                try {
                    return IMClient.getCodeForConceptId(conceptId);
                } catch (Exception ex) {
                    if (lives <= 0) {
                        throw ex;
                    }

                    LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private static String createCacheKey(String scheme, String code) {
        return scheme + ":" + code;
    }
}
