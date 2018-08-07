package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.hl7.fhir.instance.model.Condition;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class ConditionResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(ConditionResourceCache.class);

    private ResourceCache<Long, ConditionBuilder> conditionResourceCache = new ResourceCache<>();
    private Set<Long> conditionsToDelete = new HashSet<>();

    public ConditionBuilder getConditionBuilderAndRemoveFromCache(CsvCell codeIdCell, TppCsvHelper csvHelper) throws Exception {

        Long key = codeIdCell.getLong();

        ConditionBuilder cachedBuilder = conditionResourceCache.getAndRemoveFromCache(key);
        if (cachedBuilder != null) {
            return cachedBuilder;
        }

        Condition condition = (Condition) csvHelper.retrieveResource(codeIdCell.getString(), ResourceType.Condition);
        if (condition != null) {
            return new ConditionBuilder(condition);
        }

        //if the Condition doesn't exist yet, create a new one
        ConditionBuilder conditionBuilder = new ConditionBuilder();
        conditionBuilder.setId(codeIdCell.getString(), codeIdCell);
        conditionBuilder.setAsProblem(false); //default to just regular condition
        return conditionBuilder;
    }

    public boolean containsCondition(CsvCell codeIdCell) {
        Long key = codeIdCell.getLong();
        return conditionResourceCache.contains(key);
    }

    public void returnToCache(CsvCell codeIdCell, ConditionBuilder conditionBuilder) throws Exception {
        Long key = codeIdCell.getLong();
        conditionResourceCache.addToCache(key, conditionBuilder);
    }


    public void returnToCacheForDelete(CsvCell codeIdCell, ConditionBuilder conditionBuilder) throws Exception {
        Long key = codeIdCell.getLong();

        conditionResourceCache.addToCache(key, conditionBuilder);
        conditionsToDelete.add(key);
    }

    public void fileConditionResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Long codeId: conditionResourceCache.keySet()) {
            ConditionBuilder conditionBuilder = conditionResourceCache.getAndRemoveFromCache(codeId);

            boolean mapIds = !conditionBuilder.isIdMapped();
            if (conditionsToDelete.contains(codeId)) {
                fhirResourceFiler.deletePatientResource(null, mapIds, conditionBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, mapIds, conditionBuilder);
            }
        }
    }

}
