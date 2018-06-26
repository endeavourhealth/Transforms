package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.CLEVE;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.ReferenceList;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.dsig.TransformException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class CLEVETransformer {
    private static final Logger LOG = LoggerFactory.getLogger(CLEVETransformer.class);

    private static final String[] comparators = {"<=", "<", ">=", ">"};

    private static final SimpleDateFormat resultDateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createObservation((CLEVE)parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createObservation(CLEVE parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        if (true) {
            throw new TransformException("Stopping transform for now until mapping can be built in");
        }

        CsvCell clinicalEventId = parser.getEventId();

        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            //if non-active (i.e. deleted) then we don't get a personID, so need to retrieve the existing instance
            //of the resource in order to delete it
            deleteObservation(clinicalEventId, parser, csvHelper, fhirResourceFiler);
            return;
        }

        CsvCell personId = parser.getPersonId();

        // Order : first handle inactive records, so we need Patient
        ObservationBuilder observationBuilder = new ObservationBuilder();
        observationBuilder.setId(clinicalEventId.getString(), clinicalEventId);

        Reference patientReference = csvHelper.createPatientReference(personId);
        observationBuilder.setPatient(patientReference, personId);

        // check encounter data
        CsvCell encounterIdCell = parser.getEncounterId();
        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterIdCell.getString());
        observationBuilder.setEncounter(encounterReference, encounterIdCell);

        //there are lots of events that are still active but have a result text of DELETED
        CsvCell resultTextCell = parser.getEventResultText();
        if (!resultTextCell.isEmpty()
                && resultTextCell.getString().equalsIgnoreCase("DELETED")) {

            /*if (logProgress) {
                LOG.debug("" + FhirSerializationHelper.serializeResource(observationBuilder.getResource()));
                LOG.debug("DELETING RESOURCE");
            }*/

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), observationBuilder);
            return;
        }

        /*if (logProgress) {
            LOG.debug("-------1");
        }*/

        //TODO we need to filter out any records that are not final
        observationBuilder.setStatus(Observation.ObservationStatus.FINAL);

        // Performer
        CsvCell clinicianId = parser.getEventPerformedPersonnelId();
        if (!BartsCsvHelper.isEmptyOrIsZero(clinicianId)) {
            Reference practitionerReference = csvHelper.createPractitionerReference(clinicianId);
            observationBuilder.setClinician(practitionerReference, clinicianId);
        }

        CsvCell effectiveDate = parser.getEventPerformedDateTime();
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(effectiveDate)) {
            Date d = BartsCsvHelper.parseDate(effectiveDate);
            DateTimeType dateTimeType = new DateTimeType(d);
            observationBuilder.setEffectiveDate(dateTimeType, effectiveDate);
        }

        //link to parent observation if we have a parent event
        CsvCell parentEventId = parser.getParentEventId();
        if (!BartsCsvHelper.isEmptyOrIsZero(parentEventId)) {
            Reference parentObservationReference = ReferenceHelper.createReference(ResourceType.Observation, parentEventId.getString());
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
        if (!BartsCsvHelper.isEmptyOrIsZero(orderIdCell)) {
            Reference parentDiagnosticReportReference = ReferenceHelper.createReference(ResourceType.DiagnosticReport, orderIdCell.getString());
            observationBuilder.setParentResource(parentDiagnosticReportReference, orderIdCell);
        }

        //TODO - establish code mapping for millenium / FHIR
        CsvCell codeCell = parser.getEventCode();
        if (!codeCell.isEmpty()) {

            CodeableConceptBuilder codeableConceptBuilder = BartsCodeableConceptHelper.applyCodeDisplayTxt(codeCell, CodeValueSet.CLINICAL_CODE_TYPE, observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code, csvHelper);

            //if we have an explicit term in the CLEVE record, then set this as the text on the codeable concept
            CsvCell termCell = parser.getEventTitleText();
            if (!termCell.isEmpty()) {
                codeableConceptBuilder.setText(termCell.getString(), termCell);
            }
        }

        CsvCell resultClassCode = parser.getEventResultClassCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(resultClassCode)) {
            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_CLASS, resultClassCode);
            if (codeRef != null) {
                String codeDesc = codeRef.getCodeDispTxt();
                if (codeDesc.equals("Unauth")
                        || codeDesc.equals("Superseded")
                        || codeDesc.equals("REJECTED")
                        || codeDesc.equals("Not Done")
                        || codeDesc.equals("In Progress")
                        || codeDesc.equals("Active")
                        || codeDesc.equals("In Lab")
                        || codeDesc.equals("In Error")
                        || codeDesc.equals("Canceled") //NOTE the US spelling
                        || codeDesc.equals("Anticipated")
                        || codeDesc.equals("? Unknown")
                        ) {

                    //we can't just return out, because we may be UPDATING an Observation, in which case we should delete it
                    deleteObservation(clinicalEventId, parser, csvHelper, fhirResourceFiler);

                }
            }
        }

        if (isNumericResult(parser)) {
            transformResultNumericValue(parser, observationBuilder, csvHelper);

        } else if (isDateResult(parser)) {
            transformResultDateValue(parser, observationBuilder, csvHelper);

            /*//we can't just return out, because we may be UPDATING an Observation, in which case we should delete it now
            //until we want to handle these types of event
            deleteObservation(clinicalEventId, parser, csvHelper, fhirResourceFiler);
            return;*/

        } else {
            transformResultString(parser, observationBuilder, csvHelper);

            /*//we can't just return out, because we may be UPDATING an Observation, in which case we should delete it now
            //until we want to handle these types of event
            deleteObservation(clinicalEventId, parser, csvHelper, fhirResourceFiler);
            return;*/
        }

        CsvCell normalcyCodeCell = parser.getEventNormalcyCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(normalcyCodeCell)) {
            BartsCodeableConceptHelper.applyCodeDescTxt(normalcyCodeCell, CodeValueSet.CLINICAL_EVENT_NORMALCY, observationBuilder, CodeableConceptBuilder.Tag.Observation_Range_Meaning, csvHelper);
        }

        CsvCell eventTagCell = parser.getEventTag();
        if (!eventTagCell.isEmpty()) {
            String eventTagStr = eventTagCell.getString();
            String resultTextStr = resultTextCell.getString();

            //the event tag sometimes replicates what's in the result text, so only carry over if different
            if (!eventTagStr.equals(resultTextStr)) {
                observationBuilder.setNotes(eventTagStr, eventTagCell);
            }
        }

        // save resource
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder);

    }


    private static void transformResultString(CLEVE parser, ObservationBuilder observationBuilder, BartsCsvHelper csvHelper) {
        CsvCell resultTextCell = parser.getEventResultText();
        if (!resultTextCell.isEmpty()) {
            String resultText = resultTextCell.getString();

            observationBuilder.setValueString(resultText, resultTextCell);
        }
    }

    private static boolean isDateResult(CLEVE parser) {
        CsvCell resultDateCell = parser.getEventResultDateTime();
        if (resultDateCell.isEmpty()) {
            return false;
        }

        return true;
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
            //LOG.warn("Unknown precision code at start of result text [" + resultText + "]");
            TransformWarnings.log(LOG, parser, "Unknown precision code at start of result text {}", resultText);
        }

        observationBuilder.setValueDate(dateTimeType, resultTextCell, resultDateCell);
    }

    private static boolean isNumericResult(CLEVE parser) {

        //confusingly, the numeric values are in BOTH the result value and result text cells, and the
        //version in the result text cell has BETTER precision than the result value cell

        CsvCell resultValueCell = parser.getEventResultNumber();
        if (resultValueCell.isEmpty()) {
            return false;
        }

        //despite the event class saying "numeric" we have lots of events where the result is "negative" (e.g. pregnancy tests)
        //so we need to test the value itself
        CsvCell resultTextCell = parser.getEventResultText();
        String resultText = resultTextCell.getString();
        try {
            new Double(resultText);
            return true;

        } catch (NumberFormatException nfe) {
            return false;
        }
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

                //if we failed to map the comparator String to a FHIR comparator value, then log the warning
                if (comparatorValue == null) {
                    TransformWarnings.log(LOG, parser, "Unexpected comparator string {}", comparator);
                }

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
            // LOG.warn("Failed to convert [" + resultText + "] to Double");
            TransformWarnings.log(LOG, parser, "Failed to convert {} to Double", resultText);
        }

        CsvCell unitsCodeCell = parser.getEventResultUnitsCode();
        String unitsDesc = "";
        if (!unitsCodeCell.isEmpty() && unitsCodeCell.getLong() > 0) {

            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_UNITS, unitsCodeCell);

            if (cernerCodeValueRef== null) {
                TransformWarnings.log(LOG, parser, "SEVERE: cerner code {} for eventId {} not found. Row {} Column {} ",
                        unitsCodeCell.getLong(), parser.getEventId().getString(),
                        unitsCodeCell.getRowAuditId(), unitsCodeCell.getColIndex());
            }
            if (cernerCodeValueRef != null) {
                unitsDesc = cernerCodeValueRef.getCodeDispTxt();
                observationBuilder.setValueNumberUnits(unitsDesc, unitsCodeCell);
            }
        }

        // Reference range if supplied
        CsvCell low = parser.getEventNormalRangeLow();
        CsvCell high = parser.getEventNormalRangeHigh();

        if (!low.isEmpty() || !high.isEmpty()) {
            //going by how lab results were defined in the pathology spec, if we have upper and lower bounds,
            //it's an inclusive range. If we only have one bound, then it's non-inclusive.

            //sometimes the brackets are passed down from the path system to Cerner so strip them off
            String lowParsed = low.getString().replace("(","");
            String highParsed = high.getString().replace(")","");

            try {
                if (!Strings.isNullOrEmpty(lowParsed) && !Strings.isNullOrEmpty(highParsed)) {
                    observationBuilder.setRecommendedRangeLow(new Double(lowParsed), unitsDesc, Quantity.QuantityComparator.GREATER_OR_EQUAL, low);
                    observationBuilder.setRecommendedRangeHigh(new Double(highParsed), unitsDesc, Quantity.QuantityComparator.LESS_OR_EQUAL, high);

                } else if (!Strings.isNullOrEmpty(lowParsed)) {
                    observationBuilder.setRecommendedRangeLow(new Double(lowParsed), unitsDesc, Quantity.QuantityComparator.GREATER_THAN, low);

                } else {
                    observationBuilder.setRecommendedRangeHigh(new Double(highParsed), unitsDesc, Quantity.QuantityComparator.LESS_THAN, high);
                }
            }
            catch (NumberFormatException ex) {
               // LOG.warn("Range not set for Clinical Event " + parser.getEventId().getString() + " due to invalid reference range");
                TransformWarnings.log(LOG, parser, "Range not set for clinical event due to invalid reference range. Id:{}", parser.getEventId().getString());

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

        } else if (str.equals(">")) {
            return Quantity.QuantityComparator.GREATER_THAN;

        } else {
            return null;
        }
    }

    private static void deleteObservation(CsvCell clinicalEventId, CLEVE parser, BartsCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {
        Observation existingResource = (Observation)csvHelper.retrieveResourceForLocalId(ResourceType.Observation, clinicalEventId);
        if (existingResource != null) {
            ObservationBuilder observationBuilder = new ObservationBuilder(existingResource);
            //remember to pass in false to not map IDs, since the resource is already ID mapped
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, observationBuilder);
        }
    }
}
