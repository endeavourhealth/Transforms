package org.endeavourhealth.transform.subscriber;

import com.google.common.base.Strings;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.common.HasServiceSystemAndExchangeIdI;
import org.endeavourhealth.transform.common.TransformConfig;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
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

    private static Map<Integer, String> snomedConceptForCoreDBIDCache = new ConcurrentHashMap<>();
    private static Set<Integer> nullSnomedConceptForCoreDBIDCache = ConcurrentHashMap.newKeySet();

    private static Map<String, String> snomedConceptForLegacyCodeCache = new ConcurrentHashMap<>();
    private static Set<String> nullSnomedConceptForLegacyCodeCache = ConcurrentHashMap.newKeySet();

    private static Map<String, String> mappedLegacyCodeForLegacyCodeAndTermCache = new ConcurrentHashMap<>();
    private static Set<String> nullMappedLegacyCodeForLegacyCodeAndTermCache = ConcurrentHashMap.newKeySet();

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
                    return IMClient.getMappedCoreConceptDbidForSchemeCode(scheme, code);
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
                    return IMClient.getConceptDbidForSchemeCode(scheme, code, term, true);
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
                    return IMClient.getMappedCoreConceptDbidForTypeTerm(type, term);
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
                    return IMClient.getConceptDbidForTypeTerm(type, term, true);
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

    /**
     * returns the correspondiong Snomed/DM+D concept ID for a core IM DBIB
     * will return null if called for non-core DBIDs
     */
    public static String getSnomedConceptIdForCoreDBID(HasServiceSystemAndExchangeIdI params, Resource fhirResource, Integer conceptId) throws Exception {

        if (conceptId == null) {
            return null;
        }

        //check cache
        Integer key = conceptId;
        String ret = snomedConceptForCoreDBIDCache.get(key);
        if (ret != null || nullSnomedConceptForCoreDBIDCache.contains(key)) {
            return ret;
        }

        //hit IM API
        ret = getSnomedConceptIdForCoreDBIDWithRetry(conceptId);

        //add to cache
        if (ret == null) {
            nullSnomedConceptForCoreDBIDCache.add(key);

            if (!TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                throw new TransformException("Null mapped Snomed Code for Concept Id " + conceptId + " for Resource " + fhirResource.getResourceType() + " " + fhirResource.getId());
            }

        } else {
            snomedConceptForCoreDBIDCache.put(key, ret);
        }

        return ret;
    }

    private static String getSnomedConceptIdForCoreDBIDWithRetry(Integer conceptId) throws Exception {

        try {
            lock.lock();

            //during development, we get fairly frequent timeouts, so give it a couple of attempts
            int lives = 5;

            while (true) {
                lives--;
                try {
                    return IMClient.getCodeForConceptDbid(conceptId);
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

    /**
     * returns a Snomed concept ID for a given legacy code and scheme
     * e.g. for Barts code 687309281 it will return 1240511000000106
     */
    public static String getMappedSnomedConceptForSchemeCode(String scheme, String code) throws Exception {
        if (code == null) {
            return null;
        }

        //check the cache first
        String key = createCacheKey(scheme, code);
        String ret = snomedConceptForLegacyCodeCache.get(key);
        if (ret != null
                || nullSnomedConceptForLegacyCodeCache.contains(key)) {
            return ret;
        }

        //hit the IM API
        ret = getMappedSnomedConceptForSchemeCodeWithRetry(scheme, code);

        //store in the cache
        if (ret == null) {
            nullSnomedConceptForLegacyCodeCache.add(key);
            //note that this fn is expected to return null quite often
            //so there's no logging or warning for nulls

        } else {
            snomedConceptForLegacyCodeCache.put(key, ret);
        }

        return ret;
    }

    private static String getMappedSnomedConceptForSchemeCodeWithRetry(String scheme, String code) throws Exception {
        try {
            lock.lock();

            //during development, we get fairly frequent timeouts, so give it a couple of attempts
            int lives = 5;

            while (true) {
                lives--;
                try {
                    return IMClient.getMappedCoreCodeForSchemeCode(scheme, code);
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

    /**
     * returns the mapped IM DBID for a legacy code and textual term
     * e.g. for Cerner code 687309281 and term "SARS-CoV-2 RNA DETECTED" will return the artificially
     * generated "Cerner code" 7f472a28-7374-4f49-bcd1-7fafcbb1be4c
     */
    public static String getMappedLegacyCodeForLegacyCodeAndTerm(String scheme, String context, String term) throws Exception {
        if (Strings.isNullOrEmpty(scheme)) {
            throw new Exception("Null or empty scheme");
        }
        if (Strings.isNullOrEmpty(context)) {
            throw new Exception("Null or empty context");
        }
        if (Strings.isNullOrEmpty(term)) {
            throw new Exception("Null or empty term");
        }

        //check the cache first
        String cacheKey = scheme + ":" + context + ":" + term;
        String ret = mappedLegacyCodeForLegacyCodeAndTermCache.get(cacheKey);
        if (ret != null
                || nullMappedLegacyCodeForLegacyCodeAndTermCache.contains(cacheKey)) {
            return ret;
        }

        //hit the IM API
        ret = getMappedLegacyCodeForLegacyCodeAndTermWithRetry(scheme, context, term);

        //store in the cache
        if (ret == null) {
            nullMappedLegacyCodeForLegacyCodeAndTermCache.add(cacheKey);
            //note that this fn is expected to return null quite often
            //so there's no logging or warning for nulls

        } else {
            mappedLegacyCodeForLegacyCodeAndTermCache.put(cacheKey, ret);
        }

        return ret;
    }

    private static String getMappedLegacyCodeForLegacyCodeAndTermWithRetry(String scheme, String context, String term) throws Exception {
        try {
            lock.lock();

            //during development, we get fairly frequent timeouts, so give it a couple of attempts
            int lives = 5;

            while (true) {
                lives--;
                try {
                    return IMClient.getCodeForTypeTerm(scheme, context, term, false);
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
