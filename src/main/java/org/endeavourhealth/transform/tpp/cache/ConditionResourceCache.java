package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ConditionResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(ConditionResourceCache.class);

    private static Map<Long, ConditionBuilder> conditionBuildersById = new HashMap<>();

    public static ConditionBuilder getConditionBuilder(CsvCell problemIdCell,
                                                       CsvCell patientIdCell,
                                                       TppCsvHelper csvHelper,
                                                       FhirResourceFiler fhirResourceFiler) throws Exception {

        ConditionBuilder conditionBuilder = conditionBuildersById.get(problemIdCell.getLong());
        if (conditionBuilder == null) {

            org.hl7.fhir.instance.model.Condition condition
                    = (org.hl7.fhir.instance.model.Condition) csvHelper.retrieveResource(
                    problemIdCell.getString(),
                    ResourceType.Condition);

            if (condition == null) {
                //if the Condition doesn't exist yet, create a new one
                conditionBuilder = new ConditionBuilder();
                conditionBuilder.setId(problemIdCell.getString(), problemIdCell);
                conditionBuilder.setPatient(csvHelper.createPatientReference(patientIdCell));
            } else {
                conditionBuilder = new ConditionBuilder(condition);
            }

            conditionBuildersById.put(problemIdCell.getLong(), conditionBuilder);
        }
        return conditionBuilder;
    }

    public static boolean isIdInCache(Long conditionId) {
        return conditionBuildersById.containsKey(conditionId);
    }

    public static void fileConditionResources(FhirResourceFiler fhirResourceFiler) throws Exception {
//        int count = 0;
        for (Long problemId: conditionBuildersById.keySet()) {
            ConditionBuilder conditionBuilder = conditionBuildersById.get(problemId);
            boolean mapIds = !conditionBuilder.isIdMapped();
//            List<String> problems = new ArrayList<>();
//            ResourceValidatorCondition validator = new ResourceValidatorCondition();
//            validator.validateResourceSave(conditionBuilder.getResource(), fhirResourceFiler.getServiceId(), mapIds, problems);
//            if (problems.isEmpty()) {
//                fhirResourceFiler.savePatientResource(null, mapIds, conditionBuilder);
//            } else {
//                mapIds = false;
//                for (String s: problems) {
//                    LOG.info("TPPvalidator:" + s);
//                    if (s.contains("(false)")) {
//                        mapIds = true;
//                    }
//                }
//                LOG.warn("TPPValidator: Autoset resource boolean to {}. Condition id: {}. Successfully filed {}. Problems:{}", mapIds, conditionBuilder.getResourceId(), count, problems.size());
//
//                fhirResourceFiler.savePatientResource(null, mapIds, conditionBuilder);
//            }
//            count++;
            fhirResourceFiler.savePatientResource(null, mapIds, conditionBuilder);
        }

        //clear down as everything has been saved
        conditionBuildersById.clear();
    }
}
