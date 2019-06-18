package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingClinicalEventTarget;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class ClinicalEventTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ClinicalEventTargetTransformer.class);

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

        TransformWarnings.log(LOG, csvHelper, "Target Procedures to transform to FHIR: {} for exchangeId: {}", targetClinicalEvents.size(), csvHelper.getExchangeId());

        for (StagingClinicalEventTarget targetClinicalEvent : targetClinicalEvents) {

            String uniqueId = targetClinicalEvent.getUniqueId();
            boolean isDeleted = targetClinicalEvent.isDeleted();

            boolean isAuthorised = true;

            // deleted records don't have status or result text set and no point checking we already know its a delete
            if (!isDeleted) {
                //there are lots of events that are still active but have a result text of DELETED
                String resultText = targetClinicalEvent.getEventResultTxt();
                if (!resultText.isEmpty()
                        && resultText.equalsIgnoreCase("DELETED")) {
                    isDeleted  = true;
                }

                String eventResultStatus = targetClinicalEvent.getLookupEventResultStatus();
                if (eventResultStatus.equals("Unauth")
                        || eventResultStatus.equals("Superseded")
                        || eventResultStatus.equals("REJECTED")
                        || eventResultStatus.equals("Not Done")
                        || eventResultStatus.equals("In Progress")
                        || eventResultStatus.equals("Active")
                        || eventResultStatus.equals("In Lab")
                        || eventResultStatus.equals("In Error")
                        || eventResultStatus.equals("Canceled") //NOTE the US spelling
                        || eventResultStatus.equals("Anticipated")
                        || eventResultStatus.equals("? Unknown")) {

                    isAuthorised = false;
                }
            }

            if (isDeleted || !isAuthorised) {

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


            Integer parentEventId =  targetClinicalEvent.getParentEventId();
            if (parentEventId != null) {
                Reference parentDiagnosticReportReference = ReferenceHelper.createReference(ResourceType.DiagnosticReport, parentEventId.toString());
                observationBuilder.setParentResource(parentDiagnosticReportReference);
            }

            //link to parent diagnostic report if we have an order (NOTE we don't transform the orders file as of yet, but we may as well carry over this reference)
            Integer orderId =  targetClinicalEvent.getOrderId();
            if (orderId != null) {
                Reference parentDiagnosticOrderReference = ReferenceHelper.createReference(ResourceType.DiagnosticOrder, orderId.toString());
                observationBuilder.setParentResource(parentDiagnosticOrderReference);
            }

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
            /*// can be either of these three coded types
            String procedureCodeType = targetClinicalEvent.getProcedureType().trim();
            if (procedureCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED)
                    || procedureCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED_CT)) {

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);

            } else if (procedureCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);

            } else if (procedureCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_CERNER)) {
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);
            } else {
                throw new TransformException("Unknown Procedure Target code type [" + procedureCodeType + "]");
            }*/
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

            fhirResourceFiler.savePatientResource(null, observationBuilder);

            LOG.debug("Transforming clinicalId : "+uniqueId+"  Filed");
        }
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
