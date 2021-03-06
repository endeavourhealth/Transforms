package org.endeavourhealth.transform.subscriber;

import com.google.common.base.Strings;
import org.endeavourhealth.common.utility.ExpiringCache;
import org.endeavourhealth.common.utility.ExpiringSet;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.im.models.mapping.MapColumnRequest;
import org.endeavourhealth.im.models.mapping.MapColumnValueRequest;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.common.HasServiceSystemAndExchangeIdI;
import org.endeavourhealth.transform.common.TransformConfig;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class IMHelper {
    private static final Logger LOG = LoggerFactory.getLogger(IMHelper.class);
    private static final long CACHE_DURATION = 1000 * 60 * 60; //cache objects for 2hrs
    private static final long THREAD_SLEEP_TIME = 1000 * 10; //thread sleep for 10s
    private static final int RETRY_COUNT = 12; //thread retry count


    //simpler to use just a map in memory than mess about with JCS etc.
    //there won't be so many concepts that we need to worry about limiting in size

    private static ExpiringCache<String, Integer> mappedCache = new ExpiringCache<>(CACHE_DURATION);//cache for a minute
    private static Set<String> nullMappedCache = new ExpiringSet<>(CACHE_DURATION);

    private static ExpiringCache<String, Integer> coreCache = new ExpiringCache<>(CACHE_DURATION);
    private static Set<String> nullCoreCache = new ExpiringSet<>(CACHE_DURATION);

    private static ExpiringCache<Integer, String> snomedConceptForCoreDBIDCache = new ExpiringCache<>(CACHE_DURATION);
    private static Set<Integer> nullSnomedConceptForCoreDBIDCache = new ExpiringSet<>(CACHE_DURATION);

    private static ExpiringCache<String, String> snomedConceptForLegacyCodeCache = new ExpiringCache<>(CACHE_DURATION);
    private static Set<String> nullSnomedConceptForLegacyCodeCache = new ExpiringSet<>(CACHE_DURATION);

    private static ExpiringCache<String, String> mappedLegacyCodeForLegacyCodeAndTermCache = new ExpiringCache<>(CACHE_DURATION);
    private static Set<String> nullMappedLegacyCodeForLegacyCodeAndTermCache = new ExpiringSet<>(CACHE_DURATION);

    private static ExpiringCache<String, MapResponse> mappedColumnRequestResponseCache = new ExpiringCache<>(CACHE_DURATION);
    private static Set<String> nullMappedColumnRequestResponseCache = new ExpiringSet<>(CACHE_DURATION);

    private static ExpiringCache<String, MapResponse> mappedColumnValueRequestResponseCache = new ExpiringCache<>(CACHE_DURATION);
    private static Set<String> nullMappedColumnValueRequestResponseCache = new ExpiringSet<>(CACHE_DURATION);

    private static ExpiringCache<String, String> conceptIdCache = new ExpiringCache<>(CACHE_DURATION);
    private static Set<String> nullconceptIdCache = new ExpiringSet<>(CACHE_DURATION);

    public static Integer getIMMappedConcept(HasServiceSystemAndExchangeIdI params, Resource fhirResource, String scheme, String code) throws Exception {

        if (code == null
                || scheme == null) {
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

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = RETRY_COUNT;

        while (true) {
            lives--;
            try {
                LOG.trace("Going to IM API for " + scheme + ", " + code);
                return IMClient.getMappedCoreConceptDbidForSchemeCode(scheme, code);
            } catch (Exception ex) {
                if (lives <= 0) {
                    throw new Exception("Failed to call getMappedCoreConceptDbidForSchemeCode for scheme [" + scheme + "] code [" + code + "]", ex);
                }
                Thread.sleep(THREAD_SLEEP_TIME);
                LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
            }
        }
    }

    public static Integer getIMConcept(HasServiceSystemAndExchangeIdI params, Resource fhirResource, String scheme, String code, String term) throws Exception {

        if (code == null
                || scheme == null) {
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
        ret = getConceptDbidForSchemeCodeWithRetry(scheme, code, term);

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

    private static Integer getConceptDbidForSchemeCodeWithRetry(String scheme, String code, String term) throws Exception {

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = RETRY_COUNT;

        while (true) {
            lives--;
            try {
                LOG.trace("Going to IM API for " + scheme + ", " + code + ", " + term);
                return IMClient.getConceptDbidForSchemeCode(scheme, code, term, true);
            } catch (Exception ex) {
                if (lives <= 0) {
                    throw new Exception("Failed to call getConceptDbidForSchemeCode with scheme [" + scheme + "] code [" + code + "] term [" + term + "]", ex);
                }
                Thread.sleep(THREAD_SLEEP_TIME);
                LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
            }
        }
    }

    public static Integer getIMConcept(String scheme, String code) throws Exception {

        if (code == null
                || scheme == null) {
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
        ret = getConceptDbidForSchemeCodeWithRetry(scheme, code);

        if (ret == null) {
            nullCoreCache.add(key);

            if (!TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                throw new TransformException("Null IM concept for scheme " + scheme + " and code " + code);
            }

        } else {
            coreCache.put(key, ret);
        }

        return ret;
    }

    private static Integer getConceptDbidForSchemeCodeWithRetry(String scheme, String code) throws Exception {

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = RETRY_COUNT;

        while (true) {
            lives--;
            try {
                LOG.trace("Going to IM API for " + scheme + ", " + code);
                return IMClient.getConceptDbidForSchemeCode(scheme, code);
            } catch (Exception ex) {
                if (lives <= 0) {
                    throw new Exception("Failed to call getConceptDbidForSchemeCode with scheme [" + scheme + "] code [" + code + "]", ex);
                }
                Thread.sleep(THREAD_SLEEP_TIME);
                LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
            }
        }
    }

    //specifically used to lookup IM concept Id (String) using the scheme and code - compass v1 only
    public static String getIMConceptId(String scheme, String code) throws Exception {

        if (code == null
                || scheme == null) {
            return null;
        }

        //check cache first
        String key = createCacheKey(scheme, code);
        String ret = conceptIdCache.get(key);
        if (ret != null
                || nullconceptIdCache.contains(key)) {
            return ret;
        }

        //hit the IM API
        ret = getConceptIdForSchemeCodeWithRetry(scheme, code);

        if (ret == null) {
            nullconceptIdCache.add(key);

            if (!TransformConfig.instance().isAllowMissingConceptIdsInSubscriberTransform()) {
                throw new TransformException("Null IM concept for scheme " + scheme + " and code " + code);
            }

        } else {
            conceptIdCache.put(key, ret);
        }

        return ret;
    }

    private static String getConceptIdForSchemeCodeWithRetry(String scheme, String code) throws Exception {

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = RETRY_COUNT;

        while (true) {
            lives--;
            try {
                LOG.trace("Going to IM API for " + scheme + ", " + code);
                return IMClient.getConceptIdForSchemeCode(scheme, code);
            } catch (Exception ex) {
                if (lives <= 0) {
                    throw new Exception("Failed to call getConceptIdForSchemeCode with scheme [" + scheme + "] code [" + code + "]", ex);
                }
                Thread.sleep(THREAD_SLEEP_TIME);
                LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
            }
        }
    }

    public static Integer getIMMappedConceptForTypeTerm(Resource fhirResource, String type, String term) throws Exception {

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

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = RETRY_COUNT;

        while (true) {
            lives--;
            try {
                LOG.trace("Going to IM API for " + type + ", " + term);
                return IMClient.getMappedCoreConceptDbidForTypeTerm(type, term);
            } catch (Exception ex) {
                if (lives <= 0) {
                    throw new Exception("Failed to call getMappedCoreConceptDbidForTypeTerm for type [" + type + "] term [" + term + "]", ex);
                }
                Thread.sleep(THREAD_SLEEP_TIME);
                LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
            }
        }
    }

    public static Integer getConceptDbidForTypeTerm(Resource fhirResource, String type, String term) throws Exception {

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
        ret = getConceptDbidForTypeTermWithRetry(type, term);

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

    private static Integer getConceptDbidForTypeTermWithRetry(String type, String term) throws Exception {

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = RETRY_COUNT;

        while (true) {
            lives--;
            try {
                LOG.trace("Going to IM API for " + type + ", " + term);
                return IMClient.getConceptDbidForTypeTerm(type, term, true);
            } catch (Exception ex) {
                if (lives <= 0) {
                    throw new Exception("Failed to call getConceptDbidForTypeTerm for type [" + type + "] term [" + term + "]", ex);
                }
                Thread.sleep(THREAD_SLEEP_TIME);
                LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
            }
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

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = RETRY_COUNT;

        while (true) {
            lives--;
            try {
                LOG.trace("Going to IM API for " + conceptId);
                return IMClient.getCodeForConceptDbid(conceptId);
            } catch (Exception ex) {
                if (lives <= 0) {
                    throw new Exception("Failed to call getCodeForConceptDbid for conceptId [" + conceptId + "]", ex);
                }
                Thread.sleep(THREAD_SLEEP_TIME);
                LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
            }
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

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = RETRY_COUNT;

        while (true) {
            lives--;
            try {
                LOG.trace("Going to IM API for " + scheme + ", " + code);
                return IMClient.getMappedCoreCodeForSchemeCode(scheme, code);
            } catch (Exception ex) {
                if (lives <= 0) {
                    throw new Exception("Failed to call getMappedCoreCodeForSchemeCode for scheme [" + scheme + "] code [" + code + "]", ex);
                }
                Thread.sleep(THREAD_SLEEP_TIME);
                LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
            }
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

        //during development, we get fairly frequent timeouts, so give it a couple of attempts
        int lives = RETRY_COUNT;

        while (true) {
            lives--;
            try {
                LOG.trace("Going to IM API for " + scheme + ", " + context + ", " + term);
                return IMClient.getCodeForTypeTerm(scheme, context, term, true);
            } catch (Exception ex) {
                if (lives <= 0) {
                    throw new Exception("Failed to call getCodeForTypeTerm for scheme [" + scheme + "] context [" + context + "] term [" + term + "]", ex);
                }
                Thread.sleep(THREAD_SLEEP_TIME);
                LOG.warn("Exception " + ex.getMessage() + " calling into IM - will try " + lives + " more times");
            }
        }
    }

    private static String createCacheKey(String scheme, String code) {
        return scheme + ":" + code;
    }

    private static String createMapColumnCacheKey(MapColumnRequest propertyRequest) {
        return propertyRequest.getProvider() + ":" + propertyRequest.getSystem() + ":" + propertyRequest.getSchema() + ":"
                + propertyRequest.getTable() + ":" + propertyRequest.getColumn();
    }

    private static String createMapColumnValueCacheKey(MapColumnValueRequest valueRequest) {
        return valueRequest.getProvider() + ":" + valueRequest.getSystem() + ":" + valueRequest.getSchema() + ":"
                + valueRequest.getTable() + ":" + valueRequest.getColumn() + ":"
                + valueRequest.getValue().getCode() + ":"
                + valueRequest.getValue().getScheme() + ":"
                + valueRequest.getValue().getTerm();
    }

    /*
        Returns a MapResponse for a valid MapColumnRequest, either from the DB or previously cached
     */
    public static MapResponse getIMMappedPropertyResponse(MapColumnRequest propertyRequest) throws Exception {

        //check cache first
        String mapColumnCacheKey = createMapColumnCacheKey(propertyRequest);
        MapResponse ret = mappedColumnRequestResponseCache.get(mapColumnCacheKey);
        if (ret != null
                || nullMappedColumnRequestResponseCache.contains(mapColumnCacheKey)) {
            return ret;
        }
        //then try the API
        LOG.trace("Going to IM API for " + propertyRequest);
        ret = IMClient.getMapProperty(propertyRequest);
        //store in the cache using the cache key and response
        if (ret == null) {
            nullMappedColumnRequestResponseCache.add(mapColumnCacheKey);
        } else {
            mappedColumnRequestResponseCache.put(mapColumnCacheKey, ret);
        }

        return ret;
    }

    /*
        Returns a MapResponse for a valid MapColumnValueRequest, either from the DB or previously cached
     */
    public static MapResponse getIMMappedPropertyValueResponse(MapColumnValueRequest valueRequest) throws Exception {

        //check cache first
        String mapColumnValueCacheKey = createMapColumnValueCacheKey(valueRequest);
        MapResponse ret = mappedColumnValueRequestResponseCache.get(mapColumnValueCacheKey);
        if (ret != null
                || nullMappedColumnValueRequestResponseCache.contains(mapColumnValueCacheKey)) {
            return ret;
        }
        //then try the API
        LOG.trace("Going to IM API for " + valueRequest);
        ret = IMClient.getMapPropertyValue(valueRequest);

        //store in the cache using the cache key and response
        if (ret == null) {
            nullMappedColumnValueRequestResponseCache.add(mapColumnValueCacheKey);
        } else {
            mappedColumnValueRequestResponseCache.put(mapColumnValueCacheKey, ret);
        }

        return ret;
    }
}
