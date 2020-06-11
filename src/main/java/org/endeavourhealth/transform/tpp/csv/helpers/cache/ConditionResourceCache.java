package org.endeavourhealth.transform.tpp.csv.helpers.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.hl7.fhir.instance.model.Condition;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class ConditionResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(ConditionResourceCache.class);

    private ResourceCache<Long, ConditionBuilder> conditionResourceCache = new ResourceCache<>();


    public ConditionBuilder getConditionBuilderAndRemoveFromCache(CsvCell codeIdCell, TppCsvHelper csvHelper, boolean createIfNotFound) throws Exception {

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
        if (createIfNotFound) {
            ConditionBuilder conditionBuilder = new ConditionBuilder();
            conditionBuilder.setId(codeIdCell.getString(), codeIdCell);
            conditionBuilder.setAsProblem(false); //default to just regular condition
            return conditionBuilder;
        }

        return null;
    }

    public boolean containsCondition(CsvCell codeIdCell) {
        Long key = codeIdCell.getLong();
        return conditionResourceCache.contains(key);
    }

    public void returnToCache(CsvCell codeIdCell, ConditionBuilder conditionBuilder) throws Exception {
        Long key = codeIdCell.getLong();
        conditionResourceCache.addToCache(key, conditionBuilder);
    }

    public void processRemainingProblems(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Long codeId: conditionResourceCache.keySet()) {
            ConditionBuilder conditionBuilder = conditionResourceCache.getAndRemoveFromCache(codeId);

            //if the condition builder doesn't have a patient reference set, then it's from an SRProblem record
            //where no corresponding SRCode record was found, so we don't have enough information to make it useful
            if (!conditionBuilder.hasPatient()) {
                continue;
            }

            boolean mapIds = !conditionBuilder.isIdMapped();
            fhirResourceFiler.savePatientResource(null, mapIds, conditionBuilder);
        }

        //set both to null to release memory - nothing should use this class after this point
        conditionResourceCache = null;
    }

}
