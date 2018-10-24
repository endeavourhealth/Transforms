package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class DiagnosticReportBuilder extends ResourceBuilderBase
        implements HasCodeableConceptI {

    private DiagnosticReport diagnosticReport = null;

    public DiagnosticReportBuilder() {
        this(null);
    }

    public DiagnosticReportBuilder(DiagnosticReport diagnosticReport) {
        this(diagnosticReport, null);
    }

    public DiagnosticReportBuilder(DiagnosticReport diagnosticReport, ResourceFieldMappingAudit audit) {
        super(audit);

        this.diagnosticReport = diagnosticReport;
        if (this.diagnosticReport == null) {
            this.diagnosticReport = new DiagnosticReport();
            this.diagnosticReport.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_DIAGNOSTIC_REPORT));
        }
    }

    @Override
    public DomainResource getResource() {
        return diagnosticReport;
    }

    public void setPatient(Reference patientReference, CsvCell... sourceCells) {
        this.diagnosticReport.setSubject(patientReference);

        auditValue("subject.reference", sourceCells);
    }

    public void setStatus(DiagnosticReport.DiagnosticReportStatus status, CsvCell... sourceCells) {
        this.diagnosticReport.setStatus(status);

        auditValue("status", sourceCells);
    }

    public void setFiledBy(Reference practitionerReference, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.diagnosticReport, FhirExtensionUri.DIAGNOSTIC_REPORT_FILED_BY, practitionerReference);

        auditReferenceExtension(extension, sourceCells);
    }

    public void setEncounter(Reference encounterReference, CsvCell... sourceCells) {
        this.diagnosticReport.setEncounter(encounterReference);

        auditValue("encounter.reference", sourceCells);
    }

    public void setConclusion(String notes, CsvCell... sourceCells) {
        this.diagnosticReport.setConclusion(notes);

        auditValue("conclusion", sourceCells);
    }

    public void setEffectiveDate(DateTimeType effectiveDateTimeType, CsvCell... sourceCells) {
        this.diagnosticReport.setEffective(effectiveDateTimeType);

        auditValue("effectiveDateTime", sourceCells);
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

    private boolean hasResult(Reference reference) {
        if (this.diagnosticReport.hasResult()) {
            for (Reference resultReference : diagnosticReport.getResult()) {
                if (ReferenceHelper.equals(reference, resultReference)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean addResult(Reference reference, CsvCell... sourceCells) {
        //if this result is already present, it does nothing
        if (hasResult(reference)) {
            return false;
        }
        this.diagnosticReport.getResult().add(reference);

        int index = this.diagnosticReport.getResult().size() - 1;
        auditValue("result[" + index + "].reference", sourceCells);
        return true;
    }


    public void setParentResource(Reference reference, CsvCell... sourceCells) {
        super.createOrUpdateParentResourceExtension(reference, sourceCells);
    }

    @Override
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting) {

        if (tag == CodeableConceptBuilder.Tag.Diagnostic_Report_Main_Code) {
            if (this.diagnosticReport.hasCode()) {
                if (useExisting) {
                    return diagnosticReport.getCode();
                } else {
                    throw new IllegalArgumentException("Trying to add new code to DiagnosticReport when it already has one");
                }
            }
            this.diagnosticReport.setCode(new CodeableConcept());
            return this.diagnosticReport.getCode();

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Diagnostic_Report_Main_Code) {
            return "code";

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {

        if (tag == CodeableConceptBuilder.Tag.Diagnostic_Report_Main_Code) {
            this.diagnosticReport.setCode(null);

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }
}
