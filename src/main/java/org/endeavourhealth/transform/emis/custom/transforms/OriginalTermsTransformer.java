package org.endeavourhealth.transform.emis.custom.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.transforms.careRecord.ObservationTransformer;
import org.endeavourhealth.transform.emis.custom.helpers.EmisCustomCsvHelper;
import org.endeavourhealth.transform.emis.custom.schema.OriginalTerms;
import org.hl7.fhir.instance.model.*;

import java.util.Set;
import java.util.UUID;

public class OriginalTermsTransformer {

    private static ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    public static void transform(AbstractCsvParser parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCustomCsvHelper csvHelper) throws Exception {

        while (parser.nextRecord()) {

            try {
                processRecord((OriginalTerms) parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(OriginalTerms parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCustomCsvHelper csvHelper) throws Exception {

        CsvCell patientGuidCell = parser.getPatientGuid();
        CsvCell observationGuidCell = parser.getObservationGuid();

        //this file has GUIDs in the normal style, but the regular Emis data has them with brackets, so
        //we need to create new cell objects with the adjusted content
        CsvCell adjustedPatientGuidCell = new CsvCell(patientGuidCell.getRowAuditId(), patientGuidCell.getColIndex(), "{" + patientGuidCell.getString() + "}", parser);
        CsvCell adjustedObservationGuidCell = new CsvCell(observationGuidCell.getRowAuditId(), observationGuidCell.getColIndex(), "{" + observationGuidCell.getString() + "}", parser);

        //work out what FHIR resource type the original record was saved as
        Set<ResourceType> resourceTypes = ObservationTransformer.findOriginalTargetResourceTypes(fhirResourceFiler, adjustedPatientGuidCell, adjustedObservationGuidCell);

        //we do get some records for data/patients we've never received (deleted before the extract started)
        if (resourceTypes.isEmpty()) {
            return;
        }

        for (ResourceType resourceType: resourceTypes) {

            String locallyUniqueId = EmisCsvHelper.createUniqueId(adjustedPatientGuidCell, adjustedObservationGuidCell);
            UUID globallyUniqueId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), resourceType, locallyUniqueId);

            //if we've never mapped the local ID to a EDS UI, then we've never heard of this resource before
            if (globallyUniqueId == null) {
                continue;
            }

            ResourceWrapper resourceHistory = resourceRepository.getCurrentVersion(fhirResourceFiler.getServiceId(), resourceType.toString(), globallyUniqueId);

            //if the resource has been deleted skip it
            if (resourceHistory == null
                    || resourceHistory.isDeleted()) {
                continue;
            }

            String json = resourceHistory.getResourceData();
            try {
                Resource resource = FhirSerializationHelper.deserializeResource(json);
                updateResource(parser, resource, fhirResourceFiler);

            } catch (Throwable t) {
                throw new Exception("Error deserialising " + resourceType + " " + globallyUniqueId + " (raw ID " + locallyUniqueId + ")", t);
            }
        }
    }

    private static void updateResource(OriginalTerms parser, Resource resource, FhirResourceFiler fhirResourceFiler) throws Exception {

        if (resource instanceof Procedure) {
            updateProcedure(parser, (Procedure)resource, fhirResourceFiler);

        } else if (resource instanceof AllergyIntolerance) {
            updateAllergyIntolerance(parser, (AllergyIntolerance)resource, fhirResourceFiler);

        } else if (resource instanceof FamilyMemberHistory) {
            updateFamilyMemberHistory(parser, (FamilyMemberHistory)resource, fhirResourceFiler);

        } else if (resource instanceof Immunization) {
            updateImmunization(parser, (Immunization)resource, fhirResourceFiler);

        } else if (resource instanceof DiagnosticOrder) {
            updateDiagnosticOrder(parser, (DiagnosticOrder)resource, fhirResourceFiler);

        } else if (resource instanceof Specimen) {
            updateSpecimen(parser, (Specimen)resource, fhirResourceFiler);

        } else if (resource instanceof DiagnosticReport) {
            updateDiagnosticReport(parser, (DiagnosticReport)resource, fhirResourceFiler);

        } else if (resource instanceof ReferralRequest) {
            updateReferralRequest(parser, (ReferralRequest)resource, fhirResourceFiler);

        } else if (resource instanceof Condition) {
            updateCondition(parser, (Condition)resource, fhirResourceFiler);

        } else if (resource instanceof Observation) {
            updateObservation(parser, (Observation)resource, fhirResourceFiler);

        } else {
            throw new Exception("Unsupported resource type " + resource.getClass());
        }
    }

    private static void updateProcedure(OriginalTerms parser, Procedure procedure, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell originalTermCell = parser.getOriginalTerm();

        ProcedureBuilder procedureBuilder = new ProcedureBuilder(procedure);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, procedureBuilder);
    }

    private static void updateAllergyIntolerance(OriginalTerms parser, AllergyIntolerance allergyIntolerance, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell originalTermCell = parser.getOriginalTerm();

        AllergyIntoleranceBuilder procedureBuilder = new AllergyIntoleranceBuilder(allergyIntolerance);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Allergy_Intolerance_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, procedureBuilder);
    }

    private static void updateFamilyMemberHistory(OriginalTerms parser, FamilyMemberHistory familyMemberHistory, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell originalTermCell = parser.getOriginalTerm();

        FamilyMemberHistoryBuilder procedureBuilder = new FamilyMemberHistoryBuilder(familyMemberHistory);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Family_Member_History_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, procedureBuilder);
    }

    private static void updateImmunization(OriginalTerms parser, Immunization immunization, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell originalTermCell = parser.getOriginalTerm();

        ImmunizationBuilder procedureBuilder = new ImmunizationBuilder(immunization);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Immunization_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, procedureBuilder);
    }

    private static void updateDiagnosticOrder(OriginalTerms parser, DiagnosticOrder diagnosticOrder, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell originalTermCell = parser.getOriginalTerm();

        DiagnosticOrderBuilder procedureBuilder = new DiagnosticOrderBuilder(diagnosticOrder);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Diagnostic_Order_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, procedureBuilder);
    }

    private static void updateSpecimen(OriginalTerms parser, Specimen specimen, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell originalTermCell = parser.getOriginalTerm();

        SpecimenBuilder procedureBuilder = new SpecimenBuilder(specimen);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Specimen_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, procedureBuilder);
    }

    private static void updateDiagnosticReport(OriginalTerms parser, DiagnosticReport diagnosticReport, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell originalTermCell = parser.getOriginalTerm();

        DiagnosticReportBuilder procedureBuilder = new DiagnosticReportBuilder(diagnosticReport);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Diagnostic_Report_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, procedureBuilder);
    }

    private static void updateReferralRequest(OriginalTerms parser, ReferralRequest referralRequest, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell originalTermCell = parser.getOriginalTerm();

        ReferralRequestBuilder procedureBuilder = new ReferralRequestBuilder(referralRequest);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Referral_Request_Service, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, procedureBuilder);
    }

    private static void updateCondition(OriginalTerms parser, Condition condition, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell originalTermCell = parser.getOriginalTerm();

        ConditionBuilder procedureBuilder = new ConditionBuilder(condition);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, procedureBuilder);
    }

    private static void updateObservation(OriginalTerms parser, Observation observation, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell originalTermCell = parser.getOriginalTerm();

        ObservationBuilder procedureBuilder = new ObservationBuilder(observation);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, procedureBuilder);
    }
}

