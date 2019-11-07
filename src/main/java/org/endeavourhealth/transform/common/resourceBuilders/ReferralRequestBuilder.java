package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.common.fhir.schema.ReferralRequestSendMode;
import org.endeavourhealth.common.fhir.schema.ReferralType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.List;

public class ReferralRequestBuilder extends ResourceBuilderBase
        implements HasCodeableConceptI, HasIdentifierI {

    private ReferralRequest referralRequest = null;

    public ReferralRequestBuilder() {
        this(null);
    }

    public ReferralRequestBuilder(ReferralRequest referralRequest) {
        this(referralRequest, null);
    }

    public ReferralRequestBuilder(ReferralRequest referralRequest, ResourceFieldMappingAudit audit) {
        super(audit);

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

        int index = this.referralRequest.getRecipient().size() - 1;
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
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting) {

        if (tag == CodeableConceptBuilder.Tag.Referral_Request_Service) {
            //although the FHIR resource supports multiple codeable concepts, we only want to use a single one
            if (this.referralRequest.hasServiceRequested()) {
                if (useExisting) {
                    return referralRequest.getServiceRequested().get(0);
                } else {
                    throw new IllegalArgumentException("Trying to add service requested to ReferralRequest that already has one");
                }
            }
            return this.referralRequest.addServiceRequested();

        } else {
            throw new IllegalArgumentException("CodeableConcept tag " + tag + " not recognized.");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Referral_Request_Service) {
            return "serviceRequested[0]";

        } else {
            throw new IllegalArgumentException("CodeableConcept tag " + tag + " not recognized.");
        }
    }



    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Referral_Request_Service) {
            this.referralRequest.getServiceRequested().clear();

        } else {
            throw new IllegalArgumentException("CodeableConcept tag " + tag + " not recognized.");
        }
    }

    public boolean hasCodeableConcept(CodeableConceptBuilder.Tag tag) {
        if (tag == CodeableConceptBuilder.Tag.Referral_Request_Service) {
            return this.referralRequest.hasServiceRequested();

        } else {
            throw new IllegalArgumentException("CodeableConcept tag " + tag + " not recognized.");
        }
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

    public void setRecipientServiceType(String recipientServiceType, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(recipientServiceType)) {
            ExtensionConverter.removeExtension(this.referralRequest, FhirExtensionUri.REFERRAL_REQUEST_RECIPIENT_SERVICE_TYPE);

        } else {
            Extension extension = ExtensionConverter.createOrUpdateStringExtension(this.referralRequest, FhirExtensionUri.REFERRAL_REQUEST_RECIPIENT_SERVICE_TYPE, recipientServiceType);

            super.auditStringExtension(extension, sourceCells);
        }
    }

}
