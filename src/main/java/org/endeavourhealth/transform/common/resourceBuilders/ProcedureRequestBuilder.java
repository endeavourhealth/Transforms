package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.AnnotationHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class ProcedureRequestBuilder extends ResourceBuilderBase
                                    implements HasCodeableConceptI {

    private ProcedureRequest procedureRequest = null;

    public ProcedureRequestBuilder() {
        this(null);
    }

    public ProcedureRequestBuilder(ProcedureRequest procedureRequest) {
        this.procedureRequest = procedureRequest;
        if (this.procedureRequest == null) {
            this.procedureRequest = new ProcedureRequest();
            this.procedureRequest.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PROCEDURE_REQUEST));
        }
    }

    @Override
    public DomainResource getResource() {
        return procedureRequest;
    }


    public void setPatient(Reference patientReference, CsvCell... sourceCells) {
        this.procedureRequest.setSubject(patientReference);

        auditValue("subject.reference", sourceCells);
    }

    public void setIsConfidential(boolean isConfidential, CsvCell... sourceCells) {
        createOrUpdateIsConfidentialExtension(isConfidential, sourceCells);
    }

    public void setStatus(ProcedureRequest.ProcedureRequestStatus status, CsvCell... sourceCells) {
        this.procedureRequest.setStatus(status);

        auditValue("status", sourceCells);
    }

    public void setEncounter(Reference encounterReference, CsvCell... sourceCells) {
        this.procedureRequest.setEncounter(encounterReference);

        auditValue("encounter", sourceCells);
    }

    public void setRecordedBy(Reference practitionerReference, CsvCell... sourceCells) {
        this.procedureRequest.setOrderer(practitionerReference);

        auditValue("orderer.reference", sourceCells);
    }

    public void setRecordedDateTime(Date entererdDateTime, CsvCell... sourceCells) {
        this.procedureRequest.setOrderedOn(entererdDateTime);

        auditValue("orderedOn", sourceCells);
    }

    public void setLocationTypeDesc(String typeDesc, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateStringExtension(this.procedureRequest, FhirExtensionUri.PROCEDURE_REQUEST_LOCATION_DESCRIPTION, typeDesc);

        auditStringExtension(extension, sourceCells);
    }

    public void addNotes(String notes, CsvCell... sourceCells) {
        Annotation annotation = AnnotationHelper.createAnnotation(notes);
        this.procedureRequest.addNotes(annotation);

        int index = this.procedureRequest.getNotes().size()-1;
        auditValue("notes[" + index + "].text", sourceCells);
    }

    public void setPerformer(Reference practitionerReference, CsvCell... sourceCell) {
        this.procedureRequest.setPerformer(practitionerReference);

        auditValue("performer.reference", sourceCell);
    }

    public void setScheduleFreeText(String text, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateStringExtension(this.procedureRequest, FhirExtensionUri.PROCEDURE_REQUEST_SCHEDULE_TEXT, text);

        auditStringExtension(extension, sourceCells);
    }

    public void setScheduledDate(DateTimeType date, CsvCell... sourceCells) {
        this.procedureRequest.setScheduled(date);

        auditValue("scheduledDateTime", sourceCells);
    }

    /*public void setCodeText(String term, CsvCell... sourceCells) {
        getOrCreateCodeableConcept(null).setText(term);

        auditValue("code.text", sourceCells);
    }*/

    @Override
    public CodeableConcept createNewCodeableConcept(String tag) {
        if (this.procedureRequest.hasCode()) {
            throw new IllegalArgumentException("Trying to add code to ProcedureRequest when it already has one");
        } else {
            CodeableConcept codeableConcept = new CodeableConcept();
            this.procedureRequest.setCode(codeableConcept);
            return codeableConcept;
        }
    }

    @Override
    public String getCodeableConceptJsonPath(String tag, CodeableConcept codeableConcept) {
        return "code";
    }

    @Override
    public void removeCodeableConcepts(String tag) {
        this.procedureRequest.setCode(null);
    }
}
