package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingClinicalEventTarget;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.criteria.CriteriaBuilder;
import javax.xml.crypto.dsig.Transform;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class ClinicalEventTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ClinicalEventTargetTransformer.class);

    //private static final String RESULT_OBSERVATION_ID_SUFFIX = "_RESULT";

    public static void transform(FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            processsClinicalEvents(fhirResourceFiler, csvHelper);
        } catch (Exception ex) {
            fhirResourceFiler.logTransformRecordError(ex, null);
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processsClinicalEvents(FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // retrieve the target procedures for the current exchangeId
        List<StagingClinicalEventTarget> targetClinicalEvents = csvHelper.retrieveTargetClinicalEvents();
        if (targetClinicalEvents == null) {
            return;
        }

        int size = targetClinicalEvents.size();
        int done = 0;
        TransformWarnings.log(LOG, csvHelper, "Target Clinical events to transform to FHIR: {} for exchangeId: {}", new Integer(size), csvHelper.getExchangeId());

        for (StagingClinicalEventTarget targetClinicalEvent : targetClinicalEvents) {
            createObservationFromTarget(targetClinicalEvent, fhirResourceFiler, csvHelper);

            done ++;
            if (done % 1000 == 0) {
                LOG.debug("Done " + done + " of " + size);
            }
        }
    }

    private static void createObservationFromTarget(StagingClinicalEventTarget target, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        String uniqueId = target.getUniqueId();
        boolean isDeleted = target.isDeleted();

        if (isDeleted) {

            // retrieve the existing Procedure resource to perform the deletion on
            Observation existingObservation = (Observation) csvHelper.retrieveResourceForLocalId(ResourceType.Observation, uniqueId);
            if (existingObservation != null) {
                ObservationBuilder observationBuilder = new ObservationBuilder(existingObservation, target.getAuditJson());
                fhirResourceFiler.deletePatientResource(null, false, observationBuilder); //pass in false since this procedure is already ID mapped
            }

            //see if this observation was previously duplicated with a "result" observation and delete that too
            /*String resultUniqueId = uniqueId + RESULT_OBSERVATION_ID_SUFFIX;
            Observation existingResultObservation = (Observation) csvHelper.retrieveResourceForLocalId(ResourceType.Observation, resultUniqueId);
            if (existingResultObservation != null) {
                ObservationBuilder observationBuilder = new ObservationBuilder(existingResultObservation, target.getAuditJson());
                fhirResourceFiler.deletePatientResource(null, false, observationBuilder); //pass in false since this procedure is already ID mapped
            }*/

            return;
        }

        ObservationBuilder observationBuilder = new ObservationBuilder(null, target.getAuditJson());
        observationBuilder.setId(uniqueId);

        //patient reference
        int personId = target.getPersonId();
        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, "" + personId);
        observationBuilder.setPatient(patientReference);

        //encounter reference
        Integer encounterId = target.getEncounterId();
        if (encounterId != null) {
            Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, "" + encounterId);
            observationBuilder.setEncounter(encounterReference);
        }

        //we always have a performed date, so no null handling required
        DateTimeType clinicalSignificantDateTime = new DateTimeType(target.getClinicallySignificantDtTm());
        observationBuilder.setEffectiveDate(clinicalSignificantDateTime);

        if (target.getEventPerformedDtTm() != null) {
            DateTimeType eventPerformedDateTime = new DateTimeType(target.getEventPerformedDtTm());
            observationBuilder.setEffectiveDate(eventPerformedDateTime);
        }

        Long parentEventId = target.getParentEventId();
        if (parentEventId != null) {

            //events may refer to themselves as their own parent, so only set this if it's different
            long eventId = target.getEventId();
            if (parentEventId.longValue() != eventId) {
                Reference parentDiagnosticReportReference = ReferenceHelper.createReference(ResourceType.Observation, parentEventId.toString());
                observationBuilder.setParentResource(parentDiagnosticReportReference);
            }
        }

        // Order Id is referenced in the diagnostic report resource rather than the child event
        //link to parent diagnostic report if we have an order (NOTE we don't transform the orders file as of yet, but we may as well carry over this reference)
        /*Integer orderId =  targetClinicalEvent.getOrderId();
        if (orderId != null) {
            Reference parentDiagnosticOrderReference = ReferenceHelper.createReference(ResourceType.DiagnosticOrder, orderId.toString());
            observationBuilder.setParentResource(parentDiagnosticOrderReference);
        }*/

        if (target.getProcessedNumericResult() != null) {
            //if we have a numeric result, set that as the observation value
            observationBuilder.setValueNumber(target.getProcessedNumericResult());

        } else if (target.getEventResultTxt() != null) {
            //otherwise, if we have a textual result, set that as the value
            observationBuilder.setValueString(target.getEventResultTxt());

        } else {
            //this is fine
            //throw new Exception("Clinical Event " + uniqueId + " has no numeric or textual result");
        }

        if (target.getComparator() != null) {
            observationBuilder.setValueNumberComparator(convertComparator(target.getComparator()));
        }

        String unitsDesc = target.getLookupEventResultsUnitsCode();
        if (unitsDesc != null) {
            observationBuilder.setValueNumberUnits(unitsDesc);
        }

        try {
            Double lowRange = target.getNormalRangeLowValue();
            Double highRange = target.getNormalRangeHighValue();
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
        Integer performerPersonnelId = target.getEventPerformedPrsnlId();
        if (performerPersonnelId != null) {
            Reference performerReference = ReferenceHelper.createReference(ResourceType.Practitioner, String.valueOf(performerPersonnelId));
            observationBuilder.setClinician(performerReference);
        }

        // coded concept
        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);

        // All codes are cerner codes??
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);

        String procedureCode = target.getLookupEventCode();
        if (Strings.isNullOrEmpty(procedureCode)) {
            //we should always have a code ID
            throw new Exception("Clinical Event Code is empty in Clinical Event Target for Clinical Event Id " + uniqueId);
        }
        codeableConceptBuilder.setCodingCode(procedureCode);

        String procedureTerm = target.getLookupEventTerm();
        codeableConceptBuilder.setCodingDisplay(procedureTerm);
        codeableConceptBuilder.setText(procedureTerm);

        //see if Cerner code is mapped to Snomed and put that in the codeable concept too
        String mappedSnomedConceptId = IMHelper.getMappedSnomedConceptForSchemeCode(IMConstant.BARTS_CERNER, procedureCode);
        if (!Strings.isNullOrEmpty(mappedSnomedConceptId)) {

            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            codeableConceptBuilder.setCodingCode(mappedSnomedConceptId);

            //look up the term for the snomed concept ID
            String snomedTerm = TerminologyService.lookupSnomedTerm(mappedSnomedConceptId);
            if (Strings.isNullOrEmpty(snomedTerm)) {
                TransformWarnings.log(LOG, csvHelper, "Failed to find Snomed term for concept {} when processing Barts Clinical Event {}", mappedSnomedConceptId, uniqueId);
            } else {
                codeableConceptBuilder.setCodingDisplay(snomedTerm);
            }
        }


        // TODO lookup in blob table?
        String freeText = target.getEventTitleTxt();
        if (!Strings.isNullOrEmpty(freeText)) {
            observationBuilder.setNotes(freeText);
        }

        if (target.getConfidential() != null
                && target.getConfidential().booleanValue()) {
            observationBuilder.setIsConfidential(true);
        }

        updateCodeForMappedResultTexts(observationBuilder, csvHelper);

        //save resource
        fhirResourceFiler.savePatientResource(null, observationBuilder);

        /*//if our observation has a result text that's mapped to a concept in the IM, then we should create a second
        //observation for that result concept
        ObservationBuilder resultObservationBuilder = createResultObservationIfNecessary(observationBuilder, csvHelper);
        if (resultObservationBuilder != null) {
            //save both resources
            fhirResourceFiler.savePatientResource(null, observationBuilder, resultObservationBuilder);

        } else {
            //save resource
            fhirResourceFiler.savePatientResource(null, observationBuilder);
        }*/
    }

    /**
     * if the observation has a textual result that's mapped to a concept via the IM, then replace
     * the CodeableConcept with the mapped codes
     */
    private static void updateCodeForMappedResultTexts(ObservationBuilder observationBuilder, BartsCsvHelper csvHelper) throws Exception {
        Observation observation = (Observation)observationBuilder.getResource();

        //if no result text, then return false
        if (!observation.hasValue()) {
            return;
        }
        Type value = observation.getValue();
        if (!(value instanceof StringType)) {
            return;
        }
        StringType st = (StringType)value;
        String resultString = st.getValue();

        //find the Cerner Code ID from the observation
        CodeableConcept cc = observation.getCode();
        Coding cernerCodeCoding = CodeableConceptHelper.findCoding(cc, FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);
        if (cernerCodeCoding == null) {
            throw new Exception("Failed to find Cerner coding in codeable concept for observation " + observation.getId());
        }

        //if the result text isn't mapped to a concept then return out
        String cernerCode = cernerCodeCoding.getCode();
        String mappedCernerCodeForResult = IMHelper.getMappedLegacyCodeForLegacyCodeAndTerm(IMConstant.BARTS_CERNER, cernerCode, resultString);
        if (Strings.isNullOrEmpty(mappedCernerCodeForResult)) {
            TransformWarnings.log(LOG, csvHelper, "CLEVE {} result with Code ID {} and result text [{}] not mapped to concept", observation.getId(), cernerCode, resultString);
            return;
        }

        //remove the existing codeable concept that was copied from the parent
        CodeableConceptBuilder.removeExistingCodeableConcept(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code, null);

        //create a new codeable concept with the mapped code
        CodeableConceptBuilder ccBuilder = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);

        ccBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);
        ccBuilder.setCodingCode(mappedCernerCodeForResult);
        ccBuilder.setCodingDisplay(resultString);
        ccBuilder.setText(resultString);

        //if this cerner code is mapped to a snomed code, then set that in the codeable concept too
        String mappedSnomedConceptId = IMHelper.getMappedSnomedConceptForSchemeCode(IMConstant.BARTS_CERNER, mappedCernerCodeForResult);
        if (!Strings.isNullOrEmpty(mappedSnomedConceptId)) {

            ccBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            ccBuilder.setCodingCode(mappedSnomedConceptId);

            //look up the term for the snomed concept ID
            String snomedTerm = TerminologyService.lookupSnomedTerm(mappedSnomedConceptId);
            if (Strings.isNullOrEmpty(snomedTerm)) {
                TransformWarnings.log(LOG, csvHelper, "Failed to find Snomed term for concept {} when processing Barts CLEVE {}", mappedSnomedConceptId, observation.getId());
            } else {
                ccBuilder.setCodingDisplay(snomedTerm);
            }
        }

        //set result text to NULL since it's now in the CodeableConcept
        observationBuilder.setValueString(null);
    }

    /*private static ObservationBuilder createResultObservationIfNecessary(ObservationBuilder observationBuilder, BartsCsvHelper csvHelper) throws Exception {

        Observation observation = (Observation)observationBuilder.getResource();

        //if no result text, then return false
        if (!observation.hasValue()) {
            return null;
        }
        Type value = observation.getValue();
        if (!(value instanceof StringType)) {
            return null;
        }
        StringType st = (StringType)value;
        String resultString = st.getValue();

        //find the Cerner Code ID from the observation
        CodeableConcept cc = observation.getCode();
        Coding cernerCodeCoding = CodeableConceptHelper.findCoding(cc, FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);
        if (cernerCodeCoding == null) {
            throw new Exception("Failed to find Cerner coding in codeable concept for observation " + observation.getId());
        }

        String cernerCode = cernerCodeCoding.getCode();
        String mappedCernerCodeForResult = IMHelper.getMappedLegacyCodeForLegacyCodeAndTerm(IMConstant.BARTS_CERNER, cernerCode, resultString);
        if (Strings.isNullOrEmpty(mappedCernerCodeForResult)) {
            return null;
        }

        //if we've got a mapped cerner code, then we need to copy the observation and populate it with that
        String json = FhirSerializationHelper.serializeResource(observation);
        Observation copy = (Observation)FhirSerializationHelper.deserializeResource(json);
        ObservationBuilder resultObBuilder = new ObservationBuilder(copy);

        //generate a new unique local ID by adding a suffix to the existing ID
        String parentId = observation.getId();
        String newId = parentId + RESULT_OBSERVATION_ID_SUFFIX;
        resultObBuilder.setId(newId);

        //link the result to the parent observation
        Reference parentReference = ReferenceHelper.createReference(ResourceType.Observation, parentId);
        resultObBuilder.setParentResource(parentReference);

        //remove the existing codeable concept that was copied from the parent
        CodeableConceptBuilder.removeExistingCodeableConcept(resultObBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code, null);

        //create a new codeable concept with the mapped code
        CodeableConceptBuilder ccBuilder = new CodeableConceptBuilder(resultObBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);

        ccBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);
        ccBuilder.setCodingCode(mappedCernerCodeForResult);
        ccBuilder.setCodingDisplay(resultString);
        ccBuilder.setText(resultString);

        //if this cerner code is mapped to a snomed code, then set that in the codeable concept too
        String mappedSnomedConceptId = IMHelper.getMappedSnomedConceptForSchemeCode(IMConstant.BARTS_CERNER, mappedCernerCodeForResult);
        if (!Strings.isNullOrEmpty(mappedSnomedConceptId)) {

            ccBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
            ccBuilder.setCodingCode(mappedSnomedConceptId);

            //look up the term for the snomed concept ID
            String snomedTerm = TerminologyService.lookupSnomedTerm(mappedSnomedConceptId);
            if (Strings.isNullOrEmpty(snomedTerm)) {
                TransformWarnings.log(LOG, csvHelper, "Failed to find Snomed term for concept {} when processing Barts Clinical Event {}", mappedSnomedConceptId, newId);
            } else {
                ccBuilder.setCodingDisplay(snomedTerm);
            }
        }

        //set result text to NULL
        resultObBuilder.setValueString(null);

        return resultObBuilder;
    }*/



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
