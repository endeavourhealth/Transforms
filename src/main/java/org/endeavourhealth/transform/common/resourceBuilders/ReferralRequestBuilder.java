package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.common.fhir.schema.ReferralRequestSendMode;
import org.endeavourhealth.common.fhir.schema.ReferralType;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.List;

public class ReferralRequestBuilder extends ResourceBuilderBase
                                    implements HasCodeableConceptI, HasIdentifierI {

    private ReferralRequest referralRequest = null;
    public static final String TAG_REASON_CODEABLE_CONCEPT = "Reason";

    public ReferralRequestBuilder() {
        this(null);
    }

    public ReferralRequestBuilder(ReferralRequest referralRequest) {
        this.referralRequest = referralRequest;
        if (this.referralRequest == null) {
            this.referralRequest = new ReferralRequest();
            this.referralRequest.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_REFERRAL_REQUEST));
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

    public void setStatus(ReferralRequest.ReferralStatus status, CsvCell... sourceCells) {
        this.referralRequest.setStatus(status);

        auditValue("status", sourceCells);
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

        auditValue("description", sourceCells);
    }

    public String getDescription() {
        return this.referralRequest.getDescription();
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

    public void setReason(CodeableConcept codeableConcept, CsvCell... sourceCells) {
        this.referralRequest.setReason(codeableConcept);

        auditValue("reason.coding[0]", sourceCells);
    }

    public void setReasonFreeText(String freeText, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(freeText);
        this.referralRequest.setReason(codeableConcept);

        auditValue("reason.text", sourceCells);
    }

    public void setServiceRequestedFreeText(String freeText, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(freeText);
        this.referralRequest.addServiceRequested(codeableConcept);

        auditValue("serviceRequested[0].text", sourceCells);
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

        if (tag.equals(TAG_REASON_CODEABLE_CONCEPT)) {
            if (!Strings.isNullOrEmpty(this.referralRequest.getReason().getText())) {
                throw new IllegalArgumentException("Trying to add reason to referral when it already has reason " + this.referralRequest.getReason().getText());
            }

            this.referralRequest.setReason(new CodeableConcept());
            return this.referralRequest.getReason();
        }

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

    @Override
    public void removeCodeableConcept(String tag, CodeableConcept codeableConcept) {
        this.referralRequest.getServiceRequested().clear();
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

    @Override
    public List<Identifier> getIdentifiers() {
        return this.referralRequest.getIdentifier();
    }

    @Override
    public void removeIdentifier(Identifier identifier) {
        this.referralRequest.getIdentifier().remove(identifier);
    }
}
