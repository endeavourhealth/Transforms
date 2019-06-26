package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.List;

public class QuestionnaireResponseBuilder extends ResourceBuilderBase {

    private QuestionnaireResponse questionnaireResponse = null;

    public QuestionnaireResponseBuilder() {
        this(null);
    }

    public QuestionnaireResponseBuilder(QuestionnaireResponse questionnaireResponse) {
        this(questionnaireResponse, null);
    }

    public QuestionnaireResponseBuilder(QuestionnaireResponse questionnaireResponse, ResourceFieldMappingAudit audit) {
        super(audit);

        this.questionnaireResponse = questionnaireResponse;
        if (this.questionnaireResponse == null) {
            this.questionnaireResponse = new QuestionnaireResponse();
            this.questionnaireResponse.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_QUESTIONNAIRE_RESPONSE));
        }
    }

    public void setEncounter(Reference encounterReference, CsvCell... sourceCells) {
        this.questionnaireResponse.setEncounter(encounterReference);
        auditValue("encounter", sourceCells);
    }

    public void setSubject(Reference patientReference, CsvCell... sourceCells) {
        this.questionnaireResponse.setSubject(patientReference);

        auditValue("subject", sourceCells);
    }

    public void setStatus(QuestionnaireResponse.QuestionnaireResponseStatus status, CsvCell... sourceCells) {
        this.questionnaireResponse.setStatus(status);

        auditValue("status", sourceCells);
    }

    public void setIdentifier(Identifier identifier, CsvCell... sourceCells) {
        this.questionnaireResponse.setIdentifier(identifier);

        auditValue("identifier", sourceCells);
    }

    public void setAuthoredDate(Date authoredDate, CsvCell... sourceCells) {
        this.questionnaireResponse.setAuthored(authoredDate);

        auditValue("authored", sourceCells);
    }

    public QuestionnaireResponse.GroupComponent getOrCreateMainGroup() {

        if (this.questionnaireResponse.hasGroup()) {
            return this.questionnaireResponse.getGroup();
        } else {
            this.questionnaireResponse.setGroup(new QuestionnaireResponse.GroupComponent());
            return this.questionnaireResponse.getGroup();
        }
    }

    public QuestionnaireResponse.GroupComponent getGroup (QuestionnaireResponse.GroupComponent mainGroup, String groupName) {

        List<QuestionnaireResponse.GroupComponent> groups = mainGroup.getGroup();
        for (QuestionnaireResponse.GroupComponent group : groups) {

            if (group.getTitle().equalsIgnoreCase(groupName)) {
                return group;
            }
        }
        return null;
    }


    @Override
    public DomainResource getResource() {
        return questionnaireResponse;
    }

}
