package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.nems.schema.v1.RelatedPersonType;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class RelatedPersonBuilder extends ResourceBuilderBase implements
        HasContainedParametersI{
    RelatedPerson relatedPerson;


    public RelatedPersonBuilder(){
        this((RelatedPerson) null);
    }

    public RelatedPersonBuilder(RelatedPerson person){
        this(person, null);
    }

    public RelatedPersonBuilder(Reference patient){
        this();
        this.relatedPerson.setPatient(patient);
    }

    public RelatedPersonBuilder(RelatedPerson person, ResourceFieldMappingAudit auditWrapper) {
        super(auditWrapper);
        this.relatedPerson = person;
        if (this.relatedPerson == null){
            this.relatedPerson = new RelatedPerson();
//            this.relatedPerson.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_RELATED_PERSON));
        }
    }

    @Override
    public DomainResource getResource() {
        return relatedPerson;
    }
    public void setRelationship(CodeableConcept relationship){
        this.relatedPerson.setRelationship(relationship);
    }
    @Override
    public String getContainedParametersExtensionUrl() {
        return FhirExtensionUri.ADDITIONAL;
    }

    public void setDateOfBirth(Date birthDate) {
        this.relatedPerson.setBirthDate(birthDate);
    }
}
