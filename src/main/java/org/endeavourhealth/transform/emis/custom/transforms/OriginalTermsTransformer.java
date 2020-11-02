package org.endeavourhealth.transform.emis.custom.transforms;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.transforms.careRecord.ObservationTransformer;
import org.endeavourhealth.transform.emis.custom.helpers.EmisCustomCsvHelper;
import org.endeavourhealth.transform.emis.custom.schema.OriginalTerms;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

public class OriginalTermsTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(OriginalTermsTransformer.class);

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

        //this file has GUIDs in the normal style, but the regular Emis data has them with brackets and in UPPER case, so
        //we need to create new cell objects with the adjusted content
        patientGuidCell = CsvCell.factoryWithNewValue(patientGuidCell, "{" + patientGuidCell.getString().toUpperCase() + "}");
        observationGuidCell = CsvCell.factoryWithNewValue(observationGuidCell, "{" + observationGuidCell.getString().toUpperCase() + "}");

        //skip if we're filtering on patients
        if (!csvHelper.getPatientFilter().shouldProcessRecord(patientGuidCell)) {
            return;
        }

        CsvCell originalTermCell = parser.getOriginalTerm();
        CsvCurrentState currentState = parser.getCurrentState();

        OriginalTermsTransformerCallable callable = new OriginalTermsTransformerCallable(fhirResourceFiler, patientGuidCell, observationGuidCell, originalTermCell, currentState);
        csvHelper.submitToThreadPool(callable);
    }

    private static void updateResource(CsvCell originalTermCell, CsvCurrentState currentState, Resource resource, FhirResourceFiler fhirResourceFiler) throws Exception {

        if (resource instanceof Procedure) {
            updateProcedure(originalTermCell, currentState, (Procedure)resource, fhirResourceFiler);

        } else if (resource instanceof AllergyIntolerance) {
            updateAllergyIntolerance(originalTermCell, currentState, (AllergyIntolerance)resource, fhirResourceFiler);

        } else if (resource instanceof FamilyMemberHistory) {
            updateFamilyMemberHistory(originalTermCell, currentState, (FamilyMemberHistory)resource, fhirResourceFiler);

        } else if (resource instanceof Immunization) {
            updateImmunization(originalTermCell, currentState, (Immunization)resource, fhirResourceFiler);

        } else if (resource instanceof DiagnosticOrder) {
            updateDiagnosticOrder(originalTermCell, currentState, (DiagnosticOrder)resource, fhirResourceFiler);

        } else if (resource instanceof Specimen) {
            updateSpecimen(originalTermCell, currentState, (Specimen)resource, fhirResourceFiler);

        } else if (resource instanceof DiagnosticReport) {
            updateDiagnosticReport(originalTermCell, currentState, (DiagnosticReport)resource, fhirResourceFiler);

        } else if (resource instanceof ReferralRequest) {
            updateReferralRequest(originalTermCell, currentState, (ReferralRequest)resource, fhirResourceFiler);

        } else if (resource instanceof Condition) {
            updateCondition(originalTermCell, currentState, (Condition)resource, fhirResourceFiler);

        } else if (resource instanceof Observation) {
            updateObservation(originalTermCell, currentState, (Observation)resource, fhirResourceFiler);

        } else {
            throw new Exception("Unsupported resource type " + resource.getClass());
        }
    }

    private static void updateProcedure(CsvCell originalTermCell, CsvCurrentState currentState, Procedure procedure, FhirResourceFiler fhirResourceFiler) throws Exception {

        ProcedureBuilder procedureBuilder = new ProcedureBuilder(procedure);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(currentState, false, procedureBuilder);
    }

    private static void updateAllergyIntolerance(CsvCell originalTermCell, CsvCurrentState currentState, AllergyIntolerance allergyIntolerance, FhirResourceFiler fhirResourceFiler) throws Exception {

        AllergyIntoleranceBuilder procedureBuilder = new AllergyIntoleranceBuilder(allergyIntolerance);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Allergy_Intolerance_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(currentState, false, procedureBuilder);
    }

    private static void updateFamilyMemberHistory(CsvCell originalTermCell, CsvCurrentState currentState, FamilyMemberHistory familyMemberHistory, FhirResourceFiler fhirResourceFiler) throws Exception {

        FamilyMemberHistoryBuilder procedureBuilder = new FamilyMemberHistoryBuilder(familyMemberHistory);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Family_Member_History_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(currentState, false, procedureBuilder);
    }

    private static void updateImmunization(CsvCell originalTermCell, CsvCurrentState currentState, Immunization immunization, FhirResourceFiler fhirResourceFiler) throws Exception {

        ImmunizationBuilder procedureBuilder = new ImmunizationBuilder(immunization);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Immunization_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(currentState, false, procedureBuilder);
    }

    private static void updateDiagnosticOrder(CsvCell originalTermCell, CsvCurrentState currentState, DiagnosticOrder diagnosticOrder, FhirResourceFiler fhirResourceFiler) throws Exception {

        DiagnosticOrderBuilder procedureBuilder = new DiagnosticOrderBuilder(diagnosticOrder);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Diagnostic_Order_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(currentState, false, procedureBuilder);
    }

    private static void updateSpecimen(CsvCell originalTermCell, CsvCurrentState currentState, Specimen specimen, FhirResourceFiler fhirResourceFiler) throws Exception {

        SpecimenBuilder procedureBuilder = new SpecimenBuilder(specimen);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Specimen_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(currentState, false, procedureBuilder);
    }

    private static void updateDiagnosticReport(CsvCell originalTermCell, CsvCurrentState currentState, DiagnosticReport diagnosticReport, FhirResourceFiler fhirResourceFiler) throws Exception {

        DiagnosticReportBuilder procedureBuilder = new DiagnosticReportBuilder(diagnosticReport);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Diagnostic_Report_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(currentState, false, procedureBuilder);
    }

    private static void updateReferralRequest(CsvCell originalTermCell, CsvCurrentState currentState, ReferralRequest referralRequest, FhirResourceFiler fhirResourceFiler) throws Exception {

        ReferralRequestBuilder procedureBuilder = new ReferralRequestBuilder(referralRequest);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Referral_Request_Service, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(currentState, false, procedureBuilder);
    }

    private static void updateCondition(CsvCell originalTermCell, CsvCurrentState currentState, Condition condition, FhirResourceFiler fhirResourceFiler) throws Exception {

        ConditionBuilder procedureBuilder = new ConditionBuilder(condition);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(currentState, false, procedureBuilder);
    }

    private static void updateObservation(CsvCell originalTermCell, CsvCurrentState currentState, Observation observation, FhirResourceFiler fhirResourceFiler) throws Exception {

        ObservationBuilder procedureBuilder = new ObservationBuilder(observation);
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code, true);
        codeableConceptBuilder.replaceText(originalTermCell.getString(), originalTermCell);

        fhirResourceFiler.savePatientResource(currentState, false, procedureBuilder);
    }

    static class OriginalTermsTransformerCallable extends AbstractCsvCallable {

        private FhirResourceFiler fhirResourceFiler;
        private CsvCell patientGuidCell;
        private CsvCell observationGuidCell;
        private CsvCell originalTermCell;

        public OriginalTermsTransformerCallable(FhirResourceFiler fhirResourceFiler, CsvCell patientGuidCell, CsvCell observationGuidCell, CsvCell originalTermCell, CsvCurrentState currentState) {
            super(currentState);

            this.fhirResourceFiler = fhirResourceFiler;
            this.patientGuidCell = patientGuidCell;
            this.observationGuidCell = observationGuidCell;
            this.originalTermCell = originalTermCell;
        }

        @Override
        public Object call() throws Exception {

            try {
                //work out what FHIR resource type the original record was saved as
                Set<ResourceType> resourceTypes = ObservationTransformer.findOriginalTargetResourceTypes(fhirResourceFiler, patientGuidCell, observationGuidCell);

                //we do get some records for data/patients we've never received (deleted before the extract started)
                if (resourceTypes.isEmpty()) {
                    return null;
                }

                for (ResourceType resourceType : resourceTypes) {

                    String locallyUniqueId = EmisCsvHelper.createUniqueId(patientGuidCell, observationGuidCell);
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
                        updateResource(originalTermCell, this.getParserState(), resource, fhirResourceFiler);

                    } catch (Throwable t) {
                        throw new Exception("Error deserialising " + resourceType + " " + globallyUniqueId + " (raw ID " + locallyUniqueId + ")", t);
                    }
                }

            } catch (Throwable t) {
                LOG.error("Error processing original term for patient " + patientGuidCell + " and observation " + observationGuidCell, t);
                throw t;
            }

            return null;
        }
    }
}

