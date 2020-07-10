package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.CX;
import ca.uhn.hl7v2.model.v23.segment.OBR;
import ca.uhn.hl7v2.model.v23.segment.PID;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.DiagnosticReportBuilder;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiagnosticReportTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(DiagnosticReportTransformer.class);

    /**
     *
     * @param pid
     * @param fhirResourceFiler
     * @param imperialHL7Helper
     * @throws Exception
     */
    public static void createOrDeleteDiagnosticReport(PID pid, OBR obr, FhirResourceFiler fhirResourceFiler,
                                                      ImperialHL7Helper imperialHL7Helper) throws Exception {
        DiagnosticReportBuilder diagnosticReportBuilder = new DiagnosticReportBuilder();

        String observationGuid = String.valueOf(obr.getFillerOrderNumber());

        CX[] patientIdList = pid.getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());

        /*ImperialHL7Helper.setUniqueId(diagnosticReportBuilder, patientGuid, observationGuid);

        Reference patientReference = imperialHL7Helper.createPatientReference(patientGuid);*/
        /*diagnosticReportBuilder.setPatient(patientReference, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            diagnosticReportBuilder.setDeletedAudit(deletedCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), diagnosticReportBuilder);
            return;
        }

        //assume that any report already filed into Emis Web is a final report
        diagnosticReportBuilder.setStatus(DiagnosticReport.DiagnosticReportStatus.FINAL);

        CsvCell clinicianGuid = parser.getClinicianUserInRoleGuid();
        if (!clinicianGuid.isEmpty()) {
            Reference reference = imperialHL7Helper.createPractitionerReference(clinicianGuid);
            diagnosticReportBuilder.setFiledBy(reference, clinicianGuid);
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {
            Reference encounterReference = imperialHL7Helper.createEncounterReference(consultationGuid, patientGuid);
            diagnosticReportBuilder.setEncounter(encounterReference, consultationGuid);
        }

        CsvCell codeId = parser.getCodeId();
        CodeableConceptBuilder codeableConceptBuilder = EmisCodeHelper.createCodeableConcept(diagnosticReportBuilder, false, codeId, CodeableConceptBuilder.Tag.Diagnostic_Report_Main_Code, csvHelper);

        CsvCell associatedText = parser.getAssociatedText();
        if (!associatedText.isEmpty()) {
            diagnosticReportBuilder.setConclusion(associatedText.getString(), associatedText);
        }

        CsvCell effectiveDate = parser.getEffectiveDate();
        CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
        DateTimeType effectiveDateTimeType = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);
        if (effectiveDateTimeType != null) {
            diagnosticReportBuilder.setEffectiveDate(effectiveDateTimeType, effectiveDate, effectiveDatePrecision);
        }

        ReferenceList childObservations = imperialHL7Helper.getAndRemoveObservationParentRelationships(diagnosticReportBuilder.getResourceId());
        if (childObservations != null) {
            for (int i=0; i<childObservations.size(); i++) {
                Reference reference = childObservations.getReference(i);
                CsvCell[] sourceCells = childObservations.getSourceCells(i);
                diagnosticReportBuilder.addResult(reference, sourceCells);
            }
        }

        CsvCell enteredByGuid = parser.getEnteredByUserInRoleGuid();
        if (!enteredByGuid.isEmpty()) {
            Reference reference = imperialHL7Helper.createPractitionerReference(enteredByGuid);
            diagnosticReportBuilder.setRecordedBy(reference, enteredByGuid);
        }

        TS observationDate = obr.getObservationDateTime();
        if (observationDate != null) {
            diagnosticReportBuilder.setRecordedDate(entererDateTime, enteredDate, enteredTime);
        }

        CsvCell documentGuid = parser.getDocumentGuid();
        if (!documentGuid.isEmpty()) {
            Identifier fhirIdentifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.OFFICIAL, FhirIdentifierUri.IDENTIFIER_SYSTEM_EMIS_DOCUMENT_GUID, documentGuid.getString());
            diagnosticReportBuilder.addDocumentIdentifier(fhirIdentifier, documentGuid);
        }

        if (isReview(codeableConceptBuilder, parser, imperialHL7Helper, fhirResourceFiler)) {
            diagnosticReportBuilder.setIsReview(true);
        }

        CsvCell confidential = parser.getIsConfidential();
        if (confidential.getBoolean()) {
            diagnosticReportBuilder.setIsConfidential(true, confidential);
        }

        CsvCell parentObservationCell = parser.getParentObservationGuid();
        if (!parentObservationCell.isEmpty()) {
            ResourceType parentResourceType = findParentObservationType(csvHelper, fhirResourceFiler, patientGuid, parentObservationCell);
            if (parentResourceType != null) {
                Reference parentReference = ReferenceHelper.createReference(parentResourceType, imperialHL7Helper.createUniqueId(patientGuid, parentObservationCell));
                diagnosticReportBuilder.setParentResource(parentReference, parentObservationCell);
            }
        }

        //assert that these cells are empty, as we don't stored them in this resource type
        assertValueEmpty(diagnosticReportBuilder, parser);
        //in the Emis Left & Dead extracts have contained a number of records that are report headers (that transform into DiagnosticReport resources)
        //but have weird values in the min and max range fields, but no value. So continue to assert that there's
        //no value, but ignore non-empty range values
        *//*assertNumericUnitEmpty(diagnosticReportBuilder, parser);
        assertNumericRangeLowEmpty(diagnosticReportBuilder, parser);
        assertNumericRangeHighEmpty(diagnosticReportBuilder, parser);*//*

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), diagnosticReportBuilder);*/

    }

}
