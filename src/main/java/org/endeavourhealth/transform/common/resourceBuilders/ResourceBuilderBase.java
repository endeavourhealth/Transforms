package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.DomainResource;
import org.hl7.fhir.instance.model.Extension;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;

import java.util.Date;
import java.util.UUID;

public abstract class ResourceBuilderBase {

    private ResourceFieldMappingAudit auditWrapper = null;

    public ResourceBuilderBase() {
        this(null);
    }

    public ResourceBuilderBase(ResourceFieldMappingAudit auditWrapper) {
        this.auditWrapper = auditWrapper;

        if (this.auditWrapper == null) {
            this.auditWrapper = new ResourceFieldMappingAudit();
        }
    }

    /**
     * we often need to know if a resource has been through the ID mapping process,
     * so we can work out if we should be setting source IDs or mapped UUIDs in references etc.
     * so this function works that out from the resource ID
     */
    public boolean isIdMapped() {
        String id = getResourceId();
        if (Strings.isNullOrEmpty(id)) {
            throw new RuntimeException("Resource has no ID");
        }

        try {
            UUID.fromString(id);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    public void setId(String idValue, CsvCell... sourceCells) {
        getResource().setId(idValue);

        auditValue("id", sourceCells);
    }

    protected void auditStringExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        auditValue("extension[" + index + "].valueString", sourceCells);
    }

    protected void auditBooleanExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        auditValue("extension[" + index + "].valueBoolean", sourceCells);
    }

    protected void auditDateTimeExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        auditValue("extension[" + index + "].valueDateTime", sourceCells);
    }

    protected void auditDateExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        auditValue("extension[" + index + "].valueDate", sourceCells);
    }

    protected void auditReferenceExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        auditValue("extension[" + index + "].valueReference.reference", sourceCells);
    }

    protected void auditIdentifierExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        auditValue("extension[" + index + "].valueIdentifier.value", sourceCells);
    }

    protected void auditCodingExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        //assuming that the code of the coding is the interesting thing to link to
        auditValue("extension[" + index + "].valueCoding", sourceCells);
    }

    protected void auditCodeableConceptExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        //assuming that the code of the first coding is the interesting thing to audit by
        auditValue("extension[" + index + "].valueCodeableConcept.coding[0]", sourceCells);
    }

    protected void auditCodeableConceptTextExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        auditValue("extension[" + index + "].valueCodeableConcept.text", sourceCells);
    }

    protected void auditPeriodStartExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        auditValue("extension[" + index + "].valuePeriod.start", sourceCells);
    }

    protected void auditPeriodEndExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        auditValue("extension[" + index + "].valuePeriod.end", sourceCells);
    }

    protected void auditDurationExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        auditValue("extension[" + index + "].valueDuration.value", sourceCells);
    }

    protected void auditIntegerExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        auditValue("extension[" + index + "].valueInteger", sourceCells);
    }

    protected void auditDecimalExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        auditValue("extension[" + index + "].valueDecimal", sourceCells);
    }

    protected void auditQuantityValueExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        auditValue("extension[" + index + "].valueQuantity.value", sourceCells);
    }

    protected void auditQuantityUnitExtension(Extension extension, CsvCell... sourceCells) {
        int index = getResource().getExtension().indexOf(extension);
        auditValue("extension[" + index + "].valueQuantity.unit", sourceCells);
    }



    public void auditValue(String jsonField, CsvCell... sourceCells) {

        for (CsvCell csvCell: sourceCells) {
            //we can have null or empty cell passed in, so skip auditing them
            //there are also some places where dummy cell objects are created, with a row audit
            //of -1, so ignore them too
            if (csvCell == null
                    || csvCell.isEmpty()
                    || csvCell.getRowAuditId() == -1) {
                continue;
            }

            long rowAuditId = csvCell.getRowAuditId();
            int colIndex = csvCell.getColIndex();
            auditWrapper.auditValue(rowAuditId, colIndex, jsonField);
        }
    }

    public abstract DomainResource getResource();

    public String getResourceId() {
        return getResource().getId();
    }

    public ResourceFieldMappingAudit getAuditWrapper() {
        return this.auditWrapper;
    }

    /**
     * extension functions used in several resources. Deliberately "protected" so that
     * sub-classes must implement their own function to expose it, so we don't end up
     * adding these extensions to resources that shouldn't have it
     */
    protected void createOrUpdateIsConfidentialExtension(boolean isConfidential, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateBooleanExtension(getResource(), FhirExtensionUri.IS_CONFIDENTIAL, isConfidential);

        auditBooleanExtension(extension, sourceCells);
    }

    protected void createOrUpdateRecordedByExtension(Reference practitionerReference, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(getResource(), FhirExtensionUri.RECORDED_BY, practitionerReference);

        auditReferenceExtension(extension, sourceCells);
    }

    protected void createOrUpdateRecordedDateExtension(Date recordedDate, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateDateTimeExtension(getResource(), FhirExtensionUri.RECORDED_DATE, recordedDate);

        auditDateTimeExtension(extension, sourceCells);
    }

    protected void createOrUpdateDocumentIdExtension(Identifier identifier, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(getResource(), FhirExtensionUri.EXTERNAL_DOCUMENT, identifier);

        auditIdentifierExtension(extension, sourceCells);
    }

    protected void createOrUpdateIsReviewExtension(boolean isReview, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateBooleanExtension(getResource(), FhirExtensionUri.IS_REVIEW, isReview);

        auditBooleanExtension(extension, sourceCells);
    }

    public void createOrUpdateEncounterExtension(Reference reference, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(getResource(), FhirExtensionUri.ASSOCIATED_ENCOUNTER, reference);

        auditReferenceExtension(extension, sourceCells);
    }

    public void createOrUpdateParentResourceExtension(Reference reference, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(getResource(), FhirExtensionUri.PARENT_RESOURCE, reference);

        auditReferenceExtension(extension, sourceCells);
    }

    public void createOrUpdateContextExtension(String context, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateStringExtension(getResource(), FhirExtensionUri.RESOURCE_CONTEXT, context);

        auditStringExtension(extension, sourceCells);
    }





    /*public void removeAudit(String auditJsonPrefix) {
        auditWrapper.removeAudit(auditJsonPrefix);
    }*/
}
