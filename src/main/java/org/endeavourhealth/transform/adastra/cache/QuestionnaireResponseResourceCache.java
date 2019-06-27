package org.endeavourhealth.transform.adastra.cache;

import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.QuestionnaireResponseBuilder;
import org.hl7.fhir.instance.model.QuestionnaireResponse;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuestionnaireResponseResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(QuestionnaireResponseResourceCache.class);

    private ResourceCache<String, QuestionnaireResponseBuilder> questionnaireResponseBuildersByCaseId = new ResourceCache<>();

    public QuestionnaireResponseBuilder getOrCreateQuestionnaireResponseBuilder(CsvCell CaseIdCell,
                                                                       AdastraCsvHelper csvHelper,
                                                                       FhirResourceFiler fhirResourceFiler) throws Exception {

        //check the cache
        QuestionnaireResponseBuilder cachedResource
                = questionnaireResponseBuildersByCaseId.getAndRemoveFromCache(CaseIdCell.getString());
        if (cachedResource != null) {

            returnQuestionnaireResponseBuilder(CaseIdCell,cachedResource);
            return cachedResource;
        }

        QuestionnaireResponseBuilder questionnaireResponseBuilder = null;

        QuestionnaireResponse questionnaireResponse
                = (QuestionnaireResponse) csvHelper.retrieveResource(CaseIdCell.getString(), ResourceType.QuestionnaireResponse, fhirResourceFiler);
        if (questionnaireResponse == null) {
            //if the Questionnaire Response doesn't exist yet, create a new one using the Case Id
            questionnaireResponseBuilder = new QuestionnaireResponseBuilder();
            questionnaireResponseBuilder.setId(CaseIdCell.getString(), CaseIdCell);
        } else {
            questionnaireResponseBuilder = new QuestionnaireResponseBuilder(questionnaireResponse);
        }

        return questionnaireResponseBuilder;
    }

    public void cleanUpResourceCache() {
        try {
            questionnaireResponseBuildersByCaseId.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }

    public boolean questionnaireResponseBuilderInCache(CsvCell caseIdCell) {
        return questionnaireResponseBuildersByCaseId.contains(caseIdCell.getString());
    }

    public void returnQuestionnaireResponseBuilder(CsvCell caseIdCell, QuestionnaireResponseBuilder questionnaireResponseBuilder) throws Exception {
        returnQuestionnaireResponseBuilder(caseIdCell.getString(), questionnaireResponseBuilder);
    }

    public void returnQuestionnaireResponseBuilder(String caseId, QuestionnaireResponseBuilder questionnaireResponseBuilder) throws Exception {
        questionnaireResponseBuildersByCaseId.addToCache(caseId, questionnaireResponseBuilder);
    }

    public void fileQuestionnaireResponseResources(FhirResourceFiler fhirResourceFiler, AdastraCsvHelper csvHelper) throws Exception {

        for (String caseId: questionnaireResponseBuildersByCaseId.keySet()) {

            QuestionnaireResponseBuilder questionnaireResponseBuilder
                    = questionnaireResponseBuildersByCaseId.getAndRemoveFromCache(caseId);

            boolean mapIds = !(csvHelper.isResourceIdMapped(caseId, questionnaireResponseBuilder.getResource()));
            fhirResourceFiler.savePatientResource(null, mapIds, questionnaireResponseBuilder);
        }
    }
}
