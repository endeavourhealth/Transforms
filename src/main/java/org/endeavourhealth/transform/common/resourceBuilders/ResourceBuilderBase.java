package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.UUID;

public abstract class ResourceBuilderBase {

    private ResourceFieldMappingAudit auditWrapper = null;

    /*public ResourceBuilderBase() {
        this(null);
    }*/

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

    /**
     * when we delete a resource we use this fn to audit why we're deleting it. There isn't a "deleted"
     * element in the JSON, so it's not quite consistent with the auditing of regular fields, but at
     * least we have an audit of what caused it to be deleted
     */
    public void setDeletedAudit(CsvCell... sourceCells) {
        auditValue("deleted", sourceCells);
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
                    || (csvCell.getPublishedFileId() <= 0 && csvCell.getOldStyleAuditId() == null)) {
                continue;
            }

            if (csvCell.getPublishedFileId() > 0) {
                auditWrapper.auditValue(csvCell.getPublishedFileId(), csvCell.getRecordNumber(), csvCell.getColIndex(), jsonField);
            } else {
                auditWrapper.auditValueOldStyle(csvCell.getOldStyleAuditId(), csvCell.getColIndex(), jsonField);
            }
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

    protected void createOrUpdateEncounterExtension(Reference reference, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(getResource(), FhirExtensionUri.ASSOCIATED_ENCOUNTER, reference);

        auditReferenceExtension(extension, sourceCells);
    }

    protected void createOrUpdateParentResourceExtension(Reference reference, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(getResource(), FhirExtensionUri.PARENT_RESOURCE, reference);

        auditReferenceExtension(extension, sourceCells);
    }

    protected void createOrUpdateIsPrimaryExtension(boolean isPrimary, CsvCell... sourceCells) {

        if (isPrimary) {
            Extension extension = ExtensionConverter.createOrUpdateBooleanExtension(getResource(), FhirExtensionUri.IS_PRIMARY, true);
            auditBooleanExtension(extension, sourceCells);
        }
    }

    public void createOrUpdateContextExtension(String context, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateStringExtension(getResource(), FhirExtensionUri.RESOURCE_CONTEXT, context);

        auditStringExtension(extension, sourceCells);
    }

    public static ResourceBuilderBase factory(Resource resource, ResourceFieldMappingAudit audit) {
        if (resource instanceof AllergyIntolerance) {
            return new AllergyIntoleranceBuilder((AllergyIntolerance) resource, audit);

        } else if (resource instanceof Appointment) {
            return new AppointmentBuilder((Appointment) resource, audit);

        } else if (resource instanceof Condition) {
            return new ConditionBuilder((Condition) resource, audit);

        } else if (resource instanceof DiagnosticOrder) {
            return new DiagnosticOrderBuilder((DiagnosticOrder) resource, audit);

        } else if (resource instanceof DiagnosticReport) {
            return new DiagnosticReportBuilder((DiagnosticReport) resource, audit);

        } else if (resource instanceof Encounter) {
            return new EncounterBuilder((Encounter) resource, audit);

        } else if (resource instanceof EpisodeOfCare) {
            return new EpisodeOfCareBuilder((EpisodeOfCare) resource, audit);

        } else if (resource instanceof FamilyMemberHistory) {
            return new FamilyMemberHistoryBuilder((FamilyMemberHistory) resource, audit);

        } else if (resource instanceof Flag) {
            return new FlagBuilder((Flag) resource, audit);

        } else if (resource instanceof Immunization) {
            return new ImmunizationBuilder((Immunization) resource, audit);

        } else if (resource instanceof Location) {
            return new LocationBuilder((Location) resource, audit);

        } else if (resource instanceof MedicationOrder) {
            return new MedicationOrderBuilder((MedicationOrder) resource, audit);

        } else if (resource instanceof MedicationStatement) {
            return new MedicationStatementBuilder((MedicationStatement) resource, audit);

        } else if (resource instanceof Observation) {
            return new ObservationBuilder((Observation) resource, audit);

        } else if (resource instanceof Organization) {
            return new OrganizationBuilder((Organization) resource, audit);

        } else if (resource instanceof Patient) {
            return new PatientBuilder((Patient) resource, audit);

        } else if (resource instanceof Practitioner) {
            return new PractitionerBuilder((Practitioner) resource, audit);

        } else if (resource instanceof Procedure) {
            return new ProcedureBuilder((Procedure) resource, audit);

        } else if (resource instanceof ProcedureRequest) {
            return new ProcedureRequestBuilder((ProcedureRequest) resource, audit);

        } else if (resource instanceof ReferralRequest) {
            return new ReferralRequestBuilder((ReferralRequest) resource, audit);

        } else if (resource instanceof Schedule) {
            return new ScheduleBuilder((Schedule) resource, audit);

        } else if (resource instanceof Slot) {
            return new SlotBuilder((Slot) resource, audit);

        } else if (resource instanceof Specimen) {
            return new SpecimenBuilder((Specimen) resource, audit);

        } else {
            throw new RuntimeException("Unsupported Resource type " + resource.getClass());
        }
    }


    /*public void removeAudit(String auditJsonPrefix) {
        auditWrapper.removeAudit(auditJsonPrefix);
    }*/
}
