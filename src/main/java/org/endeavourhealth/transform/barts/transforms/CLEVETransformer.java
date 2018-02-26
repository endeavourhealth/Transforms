package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.CLEVE;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.ReferenceList;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class CLEVETransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(CLEVETransformer.class);

    private static final String[] comparators = {"<=", "<", ">=", ">"};
    private static final long RESULT_STATUS_AUTHORIZED = 25;
    private static final SimpleDateFormat resultDateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");

    /*
     *
     */
    public static void transform(String version,
                                 ParserI parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        if (parser == null) {
            return;
        }

        while (parser.nextRecord()) {
            try {
                createObservation((CLEVE)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
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
        ResourceId observationResourceId = getOrCreateObservationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, clinicalEventId);

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
        CsvCell clinicianId = parser.getEventPerformedPersonnelId();
        if (!clinicianId.isEmpty()) {
            ResourceId practitionerResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, clinicianId);
            Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, practitionerResourceId.getResourceId().toString());
            observationBuilder.setClinician(practitionerReference, clinicianId);
        }

        CsvCell effectiveDate = parser.getEventPerformedDateTime();
        if (!effectiveDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDate());
            observationBuilder.setEffectiveDate(dateTimeType, effectiveDate);
        }

        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterUuid.toString());
        observationBuilder.setEncounter(encounterReference, encounterIdCell);

        //link to parent observation if we have a parent event
        CsvCell parentEventId = parser.getParentEventId();
        if (!parentEventId.isEmpty()) {
            ResourceId parentObservationResourceId = getOrCreateObservationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parentEventId);
            Reference parentObservationReference = ReferenceHelper.createReference(ResourceType.Observation, parentObservationResourceId.getResourceId().toString());
            observationBuilder.setParentResource(parentObservationReference, parentEventId);
        }

        //link to child observations if we have any
        ReferenceList childReferences = csvHelper.getAndRemoveClinicalEventParentRelationships(clinicalEventId);
        if (childReferences != null) {
            for (int i=0; i<childReferences.size(); i++) {
                Reference reference = childReferences.getReference(i);
                CsvCell[] sourceCells = childReferences.getSourceCells(i);
                observationBuilder.addChildObservation(reference, sourceCells);
            }
        }

        //link to parent diagnostic report if we have an order (NOTE we don't transform the orders file as of yet, but we may as well carry over this reference)
        CsvCell orderIdCell = parser.getOrderId();
        if (!orderIdCell.isEmpty() && orderIdCell.getLong() > 0) {
            ResourceId parentDiagnosticReportId = getOrCreateDiagnosticReportResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, orderIdCell);
            Reference parentDiagnosticReportReference = ReferenceHelper.createReference(ResourceType.DiagnosticReport, parentDiagnosticReportId.getResourceId().toString());
            observationBuilder.setParentResource(parentDiagnosticReportReference, orderIdCell);
        }


        //TODO - establish code mapping for millenium / FHIR
        CsvCell codeCell = parser.getEventCode();
        CodeableConceptBuilder codeableConceptBuilder = BartsCodeableConceptHelper.applyCodeDisplayTxt(codeCell, CernerCodeValueRef.CLINICAL_CODE_TYPE, observationBuilder, ObservationBuilder.TAG_MAIN_CODEABLE_CONCEPT, csvHelper);

        //if we have an explicit term in the CLEVE record, then set this as the text on the codeable concept
        CsvCell termCell = parser.getEventTitleText();
        if (!termCell.isEmpty()) {
            codeableConceptBuilder.setText(termCell.getString(), termCell);
        }



        //TODO need to check getEventResultClassCode()
        CsvCell resultClassCode = parser.getEventResultClassCode();
        if (resultClassCode.isEmpty()
                && resultClassCode.getLong() != RESULT_STATUS_AUTHORIZED) {
            return;
        }

        CsvCell resultValueCell = parser.getEventResultNumber();
        CsvCell resultDateCell = parser.getEventResultDateTime();

        if (!resultValueCell.isEmpty()) {
            transformResultNumericValue(parser, observationBuilder, csvHelper);

        } else if (!resultDateCell.isEmpty()) {
            //TODO - restore when we want to process events with result dates
            //transformResultDateValue(parser, observationBuilder, csvHelper);
            return;

        } else {

            //TODO - remove this when we want to process more than numerics
            return;
        }


        CsvCell normalcyCodeCell = parser.getEventNormalcyCode();
        BartsCodeableConceptHelper.applyCodeDescTxt(normalcyCodeCell, CernerCodeValueRef.CLINICAL_EVENT_NORMALCY, observationBuilder, ObservationBuilder.TAG_RANGE_MEANING_CODEABLE_CONCEPT, csvHelper);

        //TODO - set comments
        CsvCell eventTagCell = parser.getEventTag();
        if (!eventTagCell.isEmpty()) {
            String eventTagStr = eventTagCell.getString();

            CsvCell resultTextCell = parser.getEventResultText();
            String resultTextStr = resultTextCell.getString();

            //the event tag sometimes replicates what's in the result text, so only carry over if different
            if (!eventTagStr.equals(resultTextStr)) {
                observationBuilder.setNotes(eventTagStr, eventTagCell);
            }
        }

        // save resource
        LOG.debug("Save Observation (PatId=" + observationBuilder.getResourceId() + "):" + FhirSerializationHelper.serializeResource(observationBuilder.getResource()));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), observationBuilder);
    }

    private static void transformResultDateValue(CLEVE parser, ObservationBuilder observationBuilder, BartsCsvHelper csvHelper) throws Exception {

        CsvCell resultDateCell = parser.getEventResultDateTime();

        //note that the date format in the parser doesn't match the format used, so convert manually here
        Date date = resultDateFormat.parse(resultDateCell.getString());

        //events with a date result have the date in both the EVENT_RESULT_DT and EVENT_RESULT_TXT column
        //except the EVENT_RESULT_TXT has additional information on the precision, so we need to use both fields
        CsvCell resultTextCell = parser.getEventResultText();
        String resultText = resultTextCell.getString();
        String precisionCode = resultText.substring(0, 1);

        DateTimeType dateTimeType = null;

        if (precisionCode.equals("0")) { //datetime
            dateTimeType = new DateTimeType(date, TemporalPrecisionEnum.MINUTE); //although the date format has seconds, it's always zero in the data, so Minute is right

        } else if (precisionCode.equals("1")) { //date
            dateTimeType = new DateTimeType(date, TemporalPrecisionEnum.DAY);

        } else if (precisionCode.equals("2")) { //time
            dateTimeType = new DateTimeType(date, TemporalPrecisionEnum.MINUTE);

        } else {
            throw new TransformException("Unknown precision code at start of result text [" + resultText + "]");
        }

        observationBuilder.setValueDate(dateTimeType, resultTextCell, resultDateCell);
    }

    private static void transformResultNumericValue(CLEVE parser, ObservationBuilder observationBuilder, BartsCsvHelper csvHelper) throws Exception {

        //numeric results have their number in both the result text column and the result number column
        //HOWEVER the result number column seems to round them to the nearest int, so it less useful. So
        //get the numeric values from the result text cell
        CsvCell resultTextCell = parser.getEventResultText();
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
            observationBuilder.setValueNumber(valueNumber, resultTextCell);

            if (comparatorValue != null) {
                observationBuilder.setValueNumberComparator(comparatorValue, resultTextCell);
            }

        } catch (NumberFormatException nfe) {
            throw new TransformException("Failed to convert [" + resultText + "] to Double");
        }

        CsvCell unitsCodeCell = parser.getEventResultUnitsCode();
        String unitsDesc = null;
        if (!unitsCodeCell.isEmpty() && unitsCodeCell.getLong() > 0) {

            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookUpCernerCodeFromCodeSet(
                    CernerCodeValueRef.CLINICAL_EVENT_UNITS,
                    unitsCodeCell.getLong());

            unitsDesc = cernerCodeValueRef.getCodeDispTxt();
            observationBuilder.setValueNumberUnits(unitsDesc, unitsCodeCell);
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
