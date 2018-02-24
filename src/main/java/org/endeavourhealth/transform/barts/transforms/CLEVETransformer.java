package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.CLEVE;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class CLEVETransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(CLEVETransformer.class);

    private static final String[] comparators = {"<=", "<", ">=", ">"};
    private static final String RESULT_STATUS_AUTHORIZED = "25";

    /*
     *
     */
    public static void transform(String version,
                                 CLEVE parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry(parser);
                if (valStr == null) {
                    createObservation(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    /*
     *
     */
    public static String validateEntry(CLEVE parser) {
        //TODO later we need more results but focus on numerics for now
        try {
            if (parser.getEventResultClassCode().equals(RESULT_STATUS_AUTHORIZED)) {
                return null;
            }
        } catch (Exception ex) {
            LOG.error("Problem parsing Event Result Status Code" + parser.toString());
        }
        return "Non-numeric result ignored for now";
    }

    public static void createObservation(CLEVE parser,
                                         FhirResourceFiler fhirResourceFiler,
                                         BartsCsvHelper csvHelper,
                                         String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        // get patient from encounter
        CsvCell encounterIdCell = parser.getEncounterId();
        UUID encounterUuid = csvHelper.findEncounterResourceIdFromEncounterId(encounterIdCell);
        UUID patientUuid = csvHelper.findPatientIdFromEncounterId(encounterIdCell);
        if (patientUuid == null) {
            LOG.warn("Skipping Clinical Event " + parser.getEventId().getString() + " due to missing encounter");
            return;
        }

        // this Observation resource id
        CsvCell clinicalEventId = parser.getEventId();
        ResourceId observationResourceId = getObservationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, clinicalEventId);

        ObservationBuilder observationBuilder = new ObservationBuilder();

        observationBuilder.setId(observationResourceId.getResourceId().toString(), clinicalEventId);

        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString());
        observationBuilder.setPatient(patientReference);

        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            LOG.debug("Delete Observation (" + observationBuilder.getResourceId() + "):" + FhirSerializationHelper.serializeResource(observationBuilder.getResource()));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), observationBuilder);
            return;
        }

        IdentifierBuilder identifierBuilder = new IdentifierBuilder(observationBuilder);
        identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
        identifierBuilder.setSystem(FhirCodeUri.CODE_SYSTEM_CERNER_OBSERVATION_ID);
        identifierBuilder.setValue(clinicalEventId.getString(), clinicalEventId);

        //TODO we need to filter out any records that are not final
        observationBuilder.setStatus(Observation.ObservationStatus.FINAL);

        // Performer
        //TODO Is IDENTIFIER_SYSTEM_BARTS_PERSONNEL_ID the right one?
        CsvCell clinicianId = parser.getClinicianID();
        if (!clinicianId.isEmpty()) {
            ResourceId clinicianResourceId = getResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, "Practitioner", clinicianId.getString());
            Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, clinicianResourceId.getResourceId().toString());
            observationBuilder.setClinician(practitionerReference, clinicianId);
        }

        CsvCell effectiveDate = parser.getEffectiveDateTime();
        if (!effectiveDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDate());
            observationBuilder.setEffectiveDate(dateTimeType, effectiveDate);
        }

        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterUuid.toString());
        observationBuilder.setEncounter(encounterReference, encounterIdCell);

        CsvCell parentEventId = parser.getParentEventId();
        if (!parentEventId.isEmpty()) {
            ResourceId parentObservationResourceId = getObservationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parentEventId);
            Reference parentObservationReference = ReferenceHelper.createReference(ResourceType.Observation, parentObservationResourceId.getResourceId().toString());
            observationBuilder.setParentResource(parentObservationReference, parentEventId);
        }

        //TODO - establish code mapping for millenium / FHIR
        CsvCell codeCell = parser.getEventCode();
        CodeableConceptBuilder codeableConceptBuilder = BartsCodeableConceptHelper.applyCodeDisplayTxt(codeCell, CernerCodeValueRef.CLINICAL_CODE_TYPE, observationBuilder, ObservationBuilder.TAG_MAIN_CODEABLE_CONCEPT, fhirResourceFiler);

        //if we have an explicit term in the CLEVE record, then set this as the text on the codeable concept
        CsvCell termCell = parser.getEventTitleText();
        if (!termCell.isEmpty()) {
            codeableConceptBuilder.setText(termCell.getString(), termCell);
        }

        CsvCell unitsCodeCell = parser.getEventResultUnitsCode();
        String unitsDesc = null;
        if (!unitsCodeCell.isEmpty() && unitsCodeCell.getLong() > 0) {

            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                    CernerCodeValueRef.CLINICAL_EVENT_UNITS,
                    unitsCodeCell.getLong(),
                    fhirResourceFiler.getServiceId());

            unitsDesc = cernerCodeValueRef.getCodeDispTxt();
            observationBuilder.setUnits(unitsDesc, unitsCodeCell);
        }

        //set value and units
        //TODO need to check getEventResultClassCode()
        // Need to store comparator separately  - new method in Quantityhelper
        // if it looks like a plain number it's simple.
        // if it contains  valid comparator symbol, use it
        CsvCell resultTextCell = parser.getEventResultText();
        if (!resultTextCell.isEmpty()) {

            //numeric results have their number in both the result text column and the result number column
            //HOWEVER the result number column seems to round them to the nearest int, so it less useful. So
            //get the numeric values from the result text cell
            String resultText = resultTextCell.getString();

            //qualified numbers are represented with a comparator at the start of the result text
            Quantity.QuantityComparator comparatorValue = null;

            for (String comparator : comparators) {
                if (resultText.startsWith(comparator)) {
                    comparatorValue = convertComparator(comparator);

                    //make sure to remove the comparator from the String, and tidy up any whitespace
                    resultText = resultText.substring(comparator.length());
                    resultText = resultText.trim();
                }
            }

            //test if the remaining result text is a number, othewise it could just have been free-text that started with a number
            try {
                //try treating it as a number
                Double valueNumber = new Double(resultText);
                observationBuilder.setValue(valueNumber, resultTextCell);

                if (comparatorValue != null) {
                    observationBuilder.setValueComparator(comparatorValue, resultTextCell);
                }

            } catch (NumberFormatException nfe) {

                //TODO - remove this when we want to process more than numerics
                return;
            }
        }

        // Reference range if supplied
        CsvCell low = parser.getEventNormalRangeLow();
        CsvCell high = parser.getEventNormalRangeHigh();

        if (!low.isEmpty() || !high.isEmpty()) {
            //going by how lab results were defined in the pathology spec, if we have upper and lower bounds,
            //it's an inclusive range. If we only have one bound, then it's non-inclusive.
            if (!low.isEmpty() && !high.isEmpty()) {
                observationBuilder.setRecommendedRangeLow(low.getDouble(), unitsDesc, Quantity.QuantityComparator.GREATER_OR_EQUAL, low);
                observationBuilder.setRecommendedRangeHigh(high.getDouble(), unitsDesc, Quantity.QuantityComparator.LESS_OR_EQUAL, high);

            } else if (!low.isEmpty()) {
                observationBuilder.setRecommendedRangeLow(low.getDouble(), unitsDesc, Quantity.QuantityComparator.GREATER_THAN, low);

            } else {
                observationBuilder.setRecommendedRangeHigh(high.getDouble(), unitsDesc, Quantity.QuantityComparator.LESS_THAN, high);
            }
        }

        CsvCell normalcyCodeCell = parser.getEventNormalcyCode();
        BartsCodeableConceptHelper.applyCodeDescTxt(normalcyCodeCell, CernerCodeValueRef.CLINICAL_EVENT_NORMALCY, observationBuilder, ObservationBuilder.TAG_RANGE_MEANING_CODEABLE_CONCEPT, fhirResourceFiler);

        //TODO - set comments
        CsvCell eventTagCell = parser.getEventTag();
        if (!eventTagCell.isEmpty()) {
            String eventTagStr = eventTagCell.getString();
            String resultTextStr = resultTextCell.getString();

            //the event tag sometimes replicates what's in the result text, so only carry over if different
            if (!eventTagStr.equals(resultTextStr)) {
                observationBuilder.setNotes(eventTagStr, eventTagCell);
            }
        }

        //TODO - remove this when we want to process more than numerics
        if (resultTextCell.isEmpty()) {
            return;
        }

        // save resource
        LOG.debug("Save Observation (PatId=" + observationBuilder.getResourceId() + "):" + FhirSerializationHelper.serializeResource(observationBuilder.getResource()));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), observationBuilder);
    }

    private static Quantity.QuantityComparator convertComparator(String str) {
        if (str.equals("<=")) {
            return Quantity.QuantityComparator.LESS_OR_EQUAL;

        } else if (str.equals("<")) {
            return Quantity.QuantityComparator.LESS_THAN;

        } else if (str.equals(">=")) {
            return Quantity.QuantityComparator.GREATER_OR_EQUAL;

        } else if (str.equals("<")) {
            return Quantity.QuantityComparator.GREATER_THAN;

        } else {
            throw new IllegalArgumentException("Unexpected comparator string [" + str + "]");
        }
    }
}
