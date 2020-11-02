package org.endeavourhealth.transform.bhrut.cache;

import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StaffCache {
    // Basic Consultant Code to consultant name cache
    private static final Logger LOG = LoggerFactory.getLogger(StaffCache.class);

    private Map<String, String> consultantCodeToName = new ConcurrentHashMap<>();

    private ResourceCache<String, PractitionerBuilder> practitionerBuilderResourceCache = new ResourceCache<>();

    public void addConsultantCode(CsvCell consultantCodeCell, CsvCell consultantName) {

        String cCode = consultantCodeCell.getString();
        String name = consultantName.getString();
        if (!consultantCodeToName.containsKey(cCode)) {
            consultantCodeToName.put(cCode, name);
        }
    }

    public boolean cCodeInCache(String cCode) {
        return consultantCodeToName.containsKey(cCode);
    }

    public String getNameForCcode(String cCode) {
        if (!cCodeInCache(cCode)) {
            return null;
        }
        return consultantCodeToName.get(cCode);
    }

    public void clear() {
        consultantCodeToName.clear();
    }

    public long size() {
        return consultantCodeToName.size();
    }


    public boolean practitionerCodeInCache(String practitionerCodeId) {
        return practitionerBuilderResourceCache.contains(practitionerCodeId);
    }

    public boolean practitionerCodeInDB(String practitionerCodeId, BhrutCsvHelper csvHelper) throws Exception {
        return (csvHelper.retrieveResource(practitionerCodeId, ResourceType.Practitioner) != null);
    }

    public PractitionerBuilder getOrCreatePractitionerBuilder(String practitionerCodeId,
                                                              BhrutCsvHelper csvHelper) throws Exception {

        PractitionerBuilder cachedResource
                = practitionerBuilderResourceCache.getAndRemoveFromCache(practitionerCodeId);
        if (cachedResource != null) {
            return cachedResource;
        }

        PractitionerBuilder practitionerBuilder = null;

        Practitioner practitioner
                = (Practitioner) csvHelper.retrieveResource(practitionerCodeId, ResourceType.Practitioner);
        if (practitioner == null) {

            practitionerBuilder = new PractitionerBuilder();
            practitionerBuilder.setId(practitionerCodeId);

        } else {
            practitionerBuilder = new PractitionerBuilder(practitioner);
        }

        return practitionerBuilder;
    }

    public void cachePractitionerBuilder(String practitionerCodeId, PractitionerBuilder practitionerBuilder) throws Exception {
        practitionerBuilderResourceCache.addToCache(practitionerCodeId, practitionerBuilder);
    }

}
