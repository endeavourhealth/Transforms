package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingClinicalEventTarget;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.criteria.CriteriaBuilder;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class ClinicalEventTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ClinicalEventTargetTransformer.class);

    private static final String BARTS_CERNER = "BartsCerner";
    private static final Integer CERNER_COVID_TEST=687309281;
    private static final String CERNER_COVID_TEST_DETECTED =       "SARS-CoV-2 RNA DETECTED";
    private static final String CERNER_COVID_TEST_NOT_DETECTED =   "SARS-CoV-2 RNA NOT detected";
    private static final String SNOMED_COVID_TEST_CODE =           "1240511000000106";
    private static final String SNOMED_COVID_TEST_POSITIVE_CODE =  "1240581000000104";
    private static final String SNOMED_COVID_TEST_NEGATIVE_CODE =  "1240591000000102";
    private static final String SNOMED_COVID_TEST_TERM = 	       "Detection of 2019 novel coronavirus using polymerase chain reaction technique (procedure)";
    private static final String SNOMED_COVID_TEST_POSITIVE_TERM =  "2019 novel coronavirus detected (finding)";
    private static final String SNOMED_COVID_TEST_NEGATIVE_TERM =  "2019 novel coronavirus not detected (finding)";

    public static void transform(FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            createClinicalEvent(fhirResourceFiler, csvHelper);
        } catch (Exception ex) {
            fhirResourceFiler.logTransformRecordError(ex, null);
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createClinicalEvent(FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // retrieve the target procedures for the current exchangeId
        List<StagingClinicalEventTarget> targetClinicalEvents = csvHelper.retrieveTargetClinicalEvents();
        if (targetClinicalEvents == null) {
            return;
        }

        TransformWarnings.log(LOG, csvHelper, "Target Clinical events to transform to FHIR: {} for exchangeId: {}", targetClinicalEvents.size(), csvHelper.getExchangeId());

        for (StagingClinicalEventTarget targetClinicalEvent : targetClinicalEvents) {

            String uniqueId = targetClinicalEvent.getUniqueId();
            boolean isDeleted = targetClinicalEvent.isDeleted();

            if (isDeleted) {

                // retrieve the existing Procedure resource to perform the deletion on
                Observation existingObservation = (Observation) csvHelper.retrieveResourceForLocalId(ResourceType.Observation, uniqueId);

                if (existingObservation != null) {
                    ObservationBuilder observationBuilder = new ObservationBuilder(existingObservation, targetClinicalEvent.getAuditJson());

                    //remember to pass in false since this existing procedure is already ID mapped
                    fhirResourceFiler.deletePatientResource(null, false, observationBuilder);
                } else {
                    TransformWarnings.log(LOG, csvHelper, "Cannot find existing Observation: {} for deletion", uniqueId);
                }

                continue;
            }

            // create the FHIR Procedure resource - NOTE
            ObservationBuilder observationBuilder = new ObservationBuilder(null, targetClinicalEvent.getAuditJson());
            observationBuilder.setId(uniqueId);

            // set the patient reference
            Integer personId = targetClinicalEvent.getPersonId();
            if (personId == null) {
                TransformWarnings.log(LOG, csvHelper, "Missing person ID in clinical_event_target for Clinical Event Id: {}", uniqueId);
                continue;
            }
            Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
            observationBuilder.setPatient(patientReference);

            // set the encounter reference
            Integer encounterId = targetClinicalEvent.getEncounterId();
            if (encounterId != null) {
                Reference encounterReference
                        = ReferenceHelper.createReference(ResourceType.Encounter, String.valueOf(encounterId));
                observationBuilder.setEncounter(encounterReference);
            }

            //we always have a performed date, so no error handling required
            DateTimeType clinicalSignificantDateTime = new DateTimeType(targetClinicalEvent.getClinicallySignificantDtTm());
            observationBuilder.setEffectiveDate(clinicalSignificantDateTime);

            if (targetClinicalEvent.getEventPerformedDtTm() != null) {
                DateTimeType eventPerformedDateTime = new DateTimeType(targetClinicalEvent.getEventPerformedDtTm());
                observationBuilder.setEffectiveDate(eventPerformedDateTime);
            }


            Long parentEventId =  targetClinicalEvent.getParentEventId();
            if (parentEventId != null) {
                Reference parentDiagnosticReportReference = ReferenceHelper.createReference(ResourceType.DiagnosticReport, parentEventId.toString());
                observationBuilder.setParentResource(parentDiagnosticReportReference);
            }

            // Order Id is referenced in the diagnostic report resource rather than the child event
            //link to parent diagnostic report if we have an order (NOTE we don't transform the orders file as of yet, but we may as well carry over this reference)
            /*Integer orderId =  targetClinicalEvent.getOrderId();
            if (orderId != null) {
                Reference parentDiagnosticOrderReference = ReferenceHelper.createReference(ResourceType.DiagnosticOrder, orderId.toString());
                observationBuilder.setParentResource(parentDiagnosticOrderReference);
            }*/

            if (targetClinicalEvent.getProcessedNumericResult() != null) {
                observationBuilder.setValueNumber(targetClinicalEvent.getProcessedNumericResult());
            }

            if (targetClinicalEvent.getComparator() != null) {
                observationBuilder.setValueNumberComparator(convertComparator(targetClinicalEvent.getComparator()));
            }

            String unitsDesc = targetClinicalEvent.getLookupEventResultsUnitsCode();
            if (unitsDesc != null) {
                observationBuilder.setValueNumberUnits(unitsDesc);
            }

            Double lowRange = targetClinicalEvent.getNormalRangeLowValue();
            Double highRange = targetClinicalEvent.getNormalRangeHighValue();

            try {
                if (lowRange != null && highRange != null) {
                    observationBuilder.setRecommendedRangeLow(lowRange, unitsDesc, Quantity.QuantityComparator.GREATER_OR_EQUAL);
                    observationBuilder.setRecommendedRangeHigh(highRange, unitsDesc, Quantity.QuantityComparator.LESS_OR_EQUAL);

                } else if (lowRange != null) {
                    observationBuilder.setRecommendedRangeLow(lowRange, unitsDesc, Quantity.QuantityComparator.GREATER_THAN);

                } else if (highRange != null){
                    observationBuilder.setRecommendedRangeHigh(highRange, unitsDesc, Quantity.QuantityComparator.LESS_THAN);
                }
            }
            catch (NumberFormatException ex) {
                // LOG.warn("Range not set for Clinical Event " + parser.getEventId().getString() + " due to invalid reference range");
                TransformWarnings.log(LOG, csvHelper, "Range not set for clinical event due to invalid reference range. Id:{}", uniqueId);

            }

            // status is always final as we check the status above
            observationBuilder.setStatus(Observation.ObservationStatus.FINAL);

            // performer and recorder
            Integer performerPersonnelId = targetClinicalEvent.getEventPerformedPrsnlId();
            if (performerPersonnelId != null) {
                Reference practitionerPerformerReference
                        = ReferenceHelper.createReference(ResourceType.Practitioner, String.valueOf(performerPersonnelId));
                observationBuilder.setClinician(practitionerPerformerReference);
            }

            // coded concept
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);

            // All codes are cerner codes??
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);

            String procedureCode = targetClinicalEvent.getLookupEventCode();
            if (!Strings.isNullOrEmpty(procedureCode)) {

                codeableConceptBuilder.setCodingCode(procedureCode);
            } else {
                TransformWarnings.log(LOG, csvHelper, "Clinical Event Code is empty in Clinical Event Target for Clinical Event Id: {}", uniqueId);
                continue;
            }
            String procedureTerm = targetClinicalEvent.getLookupEventTerm();
            codeableConceptBuilder.setCodingDisplay(procedureTerm);
            codeableConceptBuilder.setText(procedureTerm);

            // TODO lookup in blob table?
            String freeText = targetClinicalEvent.getEventTitleTxt();
            if (!Strings.isNullOrEmpty(freeText)) {
                observationBuilder.setNotes(freeText);
            }

            if (targetClinicalEvent.getConfidential() != null
                    && targetClinicalEvent.getConfidential().booleanValue()) {
                observationBuilder.setIsConfidential(true);
            }
            if (needMultipleObservations(targetClinicalEvent)) {
                   // File one Observation for the test (observationBuilder), one for the result (newObservationbBuilder).
                observationBuilder.removeCodeableConcept(CodeableConceptBuilder.Tag.Observation_Main_Code, codeableConceptBuilder.getCodeableConcept());

                Observation observation = (Observation) observationBuilder.getResource();
                String json = FhirSerializationHelper.serializeResource(observation);
                Observation newObservation = (Observation)FhirSerializationHelper.deserializeResource(json);
                newObservation.setId((observation.getId()+"-RESULT"));
                ObservationBuilder newObservationBuilder = new ObservationBuilder(newObservation);
                newObservationBuilder.setId(uniqueId+"-RESULT");

                Reference parentReference = ReferenceHelper.createReference(ResourceType.Observation, observation.getId());
                newObservationBuilder.setParentResource(parentReference);
                //Build the TEST Observation
                CodeableConceptBuilder ccBuilder = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);
                ccBuilder.setText(targetClinicalEvent.getLookupEventTerm());
                ccBuilder.addCoding((FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID));
                ccBuilder.setCodingCode(targetClinicalEvent.getLookupEventCode());
                ccBuilder.setCodingDisplay(targetClinicalEvent.getLookupEventTerm());
                String eventCode = targetClinicalEvent.getLookupEventCode();
                String cc = IMHelper.getMappedCoreCodeForSchemeCode(BARTS_CERNER,eventCode);
                ccBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT) ;
                ccBuilder.setCodingCode(cc);
                ccBuilder.setCodingDisplay(targetClinicalEvent.getLookupEventTerm());
                observationBuilder.setValueString(targetClinicalEvent.getEventResultTxt());
                fhirResourceFiler.savePatientResource(null, observationBuilder);

                //Build the RESULT Observation only if we have a result text
                String resultTxt = targetClinicalEvent.getEventResultTxt();
                if (!Strings.isNullOrEmpty(resultTxt)) {
                    String resultLegacy = IMHelper.getCodeForTypeTerm(BARTS_CERNER, eventCode, resultTxt, true);
                    String resultSnomed = IMHelper.getMappedCoreCodeForSchemeCode(BARTS_CERNER, resultLegacy);
                    newObservationBuilder.removeCodeableConcept(CodeableConceptBuilder.Tag.Observation_Main_Code, codeableConceptBuilder.getCodeableConcept());
                    CodeableConceptBuilder ccBuilderResult
                            = new CodeableConceptBuilder(newObservationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);
                    ccBuilderResult.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                    if (resultTxt.equalsIgnoreCase(CERNER_COVID_TEST_DETECTED)) {
                        String code = IMHelper.getCodeForTypeTerm(BARTS_CERNER, eventCode,
                                CERNER_COVID_TEST_DETECTED, true);
                        ccBuilderResult.setCodingCode(resultLegacy);
                        ccBuilderResult.setCodingDisplay(SNOMED_COVID_TEST_POSITIVE_TERM);
                    } else if (resultTxt.equalsIgnoreCase(CERNER_COVID_TEST_NOT_DETECTED)) {
                        //String code = IMHelper.getCodeForTypeTerm(BARTS_CERNER, eventCode, CERNER_COVID_TEST_NOT_DETECTED, true);
                        ccBuilderResult.setCodingCode(resultSnomed);
                        ccBuilderResult.setCodingDisplay(SNOMED_COVID_TEST_NEGATIVE_TERM);
                    } else {
                        TransformWarnings.log(LOG,csvHelper,"Unexpected coronavirus resultText:<" + resultTxt + ">");
                    }
                    ccBuilderResult.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);
                    String cc2 = IMHelper.getMappedCoreCodeForSchemeCode(BARTS_CERNER,targetClinicalEvent.getLookupEventCode());
                    ccBuilderResult.setCodingCode(cc2);
                    ccBuilderResult.setCodingDisplay(targetClinicalEvent.getLookupEventTerm());
                    newObservationBuilder.setValueString(targetClinicalEvent.getEventResultTxt());
                    fhirResourceFiler.savePatientResource(null, newObservationBuilder);
                }   //TODO anything here for when no/other result?

            } else {
                fhirResourceFiler.savePatientResource(null, observationBuilder);
            }

        }
    }

   private static boolean needMultipleObservations(StagingClinicalEventTarget target) {
        // So far only needed for Barts Covid
        try {
            String ret = IMHelper.getMappedCoreCodeForSchemeCode("BartsCerner", target.getLookupEventCode());
            if (ret != null) {
                LOG.debug("needMultipleObservations for " + ret);
               return true;
           }
       } catch (Exception e) {
           LOG.error("Exception calling IMHelper for type " + BARTS_CERNER + " code " + target.getLookupEventCode());
       }
        return false;
   }
    public static Date setTime235959(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    private static Quantity.QuantityComparator convertComparator(String str) {
        if (str.equals("<=")) {
            return Quantity.QuantityComparator.LESS_OR_EQUAL;

        } else if (str.equals("<")) {
            return Quantity.QuantityComparator.LESS_THAN;

        } else if (str.equals(">=")) {
            return Quantity.QuantityComparator.GREATER_OR_EQUAL;

        } else if (str.equals(">")) {
            return Quantity.QuantityComparator.GREATER_THAN;

        } else {
            return null;
        }
    }


}
