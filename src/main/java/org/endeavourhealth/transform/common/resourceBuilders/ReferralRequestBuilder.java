package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.common.fhir.schema.ReferralRequestSendMode;
import org.endeavourhealth.common.fhir.schema.ReferralType;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class ReferralRequestBuilder extends ResourceBuilderBase
                                    implements HasCodeableConceptI, HasIdentifierI {

    private ReferralRequest referralRequest = null;

    public ReferralRequestBuilder() {
        this(null);
    }

    public ReferralRequestBuilder(ReferralRequest referralRequest) {
        this.referralRequest = referralRequest;
        if (this.referralRequest == null) {
            this.referralRequest = new ReferralRequest();
            this.referralRequest.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_REFERRAL_REQUEST));
        }
    }

    @Override
    public DomainResource getResource() {
        return referralRequest;
    }

    public void setPatient(Reference patientReference, CsvCell... sourceCells) {
        this.referralRequest.setPatient(patientReference);

        auditValue("patient.reference", sourceCells);
    }

    public void setDate(DateTimeType dateTimeType, CsvCell... sourceCells) {
        this.referralRequest.setDateElement(dateTimeType);

        auditValue("date", sourceCells);
    }

    public void setEncounter(Reference encounterReference, CsvCell... sourceCells) {
        this.referralRequest.setEncounter(encounterReference);

        auditValue("encounter.reference", sourceCells);
    }


    public void setDescription(String notes, CsvCell... sourceCells) {
        this.referralRequest.setDescription(notes);
    }

    public void setRecordedBy(Reference practitionerReference, CsvCell... sourceCells) {
        createOrUpdateRecordedByExtension(practitionerReference, sourceCells);
    }

    public void setRecordedDate(Date recordedDate, CsvCell... sourceCells) {
        createOrUpdateRecordedDateExtension(recordedDate, sourceCells);
    }

    public void addDocumentIdentifier(Identifier identifier, CsvCell... sourceCells) {
        createOrUpdateDocumentIdExtension(identifier, sourceCells);
    }

    public void setIsReview(boolean isReview, CsvCell... sourceCells) {
        createOrUpdateIsReviewExtension(isReview, sourceCells);
    }

    public void setIsConfidential(boolean isConfidential, CsvCell... sourceCells) {
        createOrUpdateIsConfidentialExtension(isConfidential, sourceCells);
    }

    public Reference getRequester() {
        if (this.referralRequest.hasRequester()) {
            return this.referralRequest.getRequester();
        } else {
            return null;
        }
    }

    public void setRequester(Reference practitionerReference, CsvCell... sourceCells) {
        this.referralRequest.setRequester(practitionerReference);

        auditValue("requester.reference", sourceCells);
    }

    public void addRecipient(Reference practitionerOrOrganizationReference, CsvCell... sourceCells) {
        this.referralRequest.addRecipient(practitionerOrOrganizationReference);

        int index = this.referralRequest.getRecipient().size()-1;
        auditValue("recipient[" + index + "].reference", sourceCells);
    }

    /*public void addIdentifier(Identifier identifier, CsvCell... sourceCells) {
        this.referralRequest.addIdentifier(identifier);

        int index = this.referralRequest.getIdentifier().indexOf(identifier);
        auditValue("identifier[" + index + "].value", sourceCells);
    }*/

    public void setPriority(ReferralPriority fhirPriority, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(fhirPriority);
        this.referralRequest.setPriority(codeableConcept);

        auditValue("priority.coding[0]", sourceCells);
    }

    public void setPriorityFreeText(String freeText, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(freeText);
        this.referralRequest.setPriority(codeableConcept);

        auditValue("priority.text", sourceCells);
    }

    public void setType(ReferralType type, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(type);
        this.referralRequest.setType(codeableConcept);

        auditValue("type.coding[0]", sourceCells);
    }

    public void setTypeFreeText(String freeText, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(freeText);
        this.referralRequest.setType(codeableConcept);

        auditValue("type.text", sourceCells);
    }

    public void setMode(ReferralRequestSendMode fhirMode, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(fhirMode);
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.referralRequest, FhirExtensionUri.REFERRAL_REQUEST_SEND_MODE, codeableConcept);

        auditCodeableConceptExtension(extension, sourceCells);
    }

    public void setModeFreeText(String freeText, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(freeText);
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.referralRequest, FhirExtensionUri.REFERRAL_REQUEST_SEND_MODE, codeableConcept);

        auditCodeableConceptTextExtension(extension, sourceCells);
    }

    @Override
    public CodeableConcept createNewCodeableConcept(String tag) {

        //although the FHIR resource supports multiple codeable concepts, we only want to use a single one
        if (this.referralRequest.hasServiceRequested()) {
            throw new IllegalArgumentException("Trying to add service requested to ReferralRequest that already has one");
        }
        return this.referralRequest.addServiceRequested();
    }

    @Override
    public String getCodeableConceptJsonPath(String tag, CodeableConcept codeableConcept) {
        return "serviceRequested[0]";
    }

    public void setParentResource(Reference reference, CsvCell... sourceCells) {
        super.createOrUpdateParentResourceExtension(reference, sourceCells);
    }

    @Override
    public Identifier addIdentifier() {
        return this.referralRequest.addIdentifier();
    }

    @Override
    public String getIdentifierJsonPrefix(Identifier identifier) {
        int index = this.referralRequest.getIdentifier().indexOf(identifier);
        return "identifier[" + index + "]";
    }
}
