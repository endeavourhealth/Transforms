package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
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


        // get encounter details (should already have been created previously)
        CsvCell encounterIdCell = parser.getEncounterId();
        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterIdCell.getString());
        if (encounterResourceId == null) {
            LOG.warn("Skipping Clinical Event " + parser.getEventId().getString() + " due to missing encounter");
            return;
        }

        // get patient from encounter
        Encounter fhirEncounter = (Encounter)csvHelper.retrieveResource(encounterResourceId.getUniqueId(), ResourceType.Encounter);
        String patientReferenceValue = fhirEncounter.getPatient().getReference();

        // this Observation resource id
        CsvCell clinicalEventId = parser.getEventId();
        ResourceId observationResourceId = getObservationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, clinicalEventId);

        ObservationBuilder observationBuilder = new ObservationBuilder();

        observationBuilder.setId(observationResourceId.getResourceId().toString(), clinicalEventId);

        Reference patientReference = ReferenceHelper.createReference(patientReferenceValue);
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

        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString());
        observationBuilder.setEncounter(encounterReference, encounterIdCell);

        CsvCell parentEventId = parser.getParentEventId();
        if (!parentEventId.isEmpty()) {
            ResourceId parentObservationResourceId = getObservationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parentEventId);
            Reference parentObservationReference = ReferenceHelper.createReference(ResourceType.Observation, parentObservationResourceId.getResourceId().toString());
            observationBuilder.setParentResource(parentObservationReference, parentEventId);
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(observationBuilder, ObservationBuilder.TAG_MAIN_CODEABLE_CONCEPT);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);

        //TODO - establish code mapping for millenium / FHIR
        CsvCell codeCell = parser.getEventCode();
        CsvCell termCell = parser.getEventTitleText();
        if (!codeCell.isEmpty() && codeCell.getLong() > 0) {

            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                                                                CernerCodeValueRef.CLINICAL_CODE_TYPE,
                                                                codeCell.getLong(),
                                                                fhirResourceFiler.getServiceId());

            String officialTerm = cernerCodeValueRef.getCodeDispTxt();

            codeableConceptBuilder.setCodingCode(codeCell.getString(), codeCell);
            codeableConceptBuilder.setCodingDisplay(officialTerm);
        }

        //if we have an explicit term in the CLEVE record, then set this as the text on the codeable concept
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
        if (!normalcyCodeCell.isEmpty() && normalcyCodeCell.getLong() > 0) {

            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                    CernerCodeValueRef.CLINICAL_EVENT_NORMALCY,
                    normalcyCodeCell.getLong(),
                    fhirResourceFiler.getServiceId());

            String normalcyDesc = cernerCodeValueRef.getCodeDescTxt();

            CodeableConceptBuilder normalcyBuilder = new CodeableConceptBuilder(observationBuilder, ObservationBuilder.TAG_RANGE_MEANING_CODEABLE_CONCEPT);
            normalcyBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);
            normalcyBuilder.setCodingCode(normalcyCodeCell.getString(), normalcyCodeCell);
            normalcyBuilder.setCodingDisplay(normalcyDesc);
        }

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

    /*
     *
     */
    /*public static void createObservation(CLEVE parser,
                                         FhirResourceFiler fhirResourceFiler,
                                         BartsCsvHelper csvHelper,
                                         String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Patient
        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(parser.getPatientId()))};
        ResourceId patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getPatientId(), null, null, null, null, null, organisationResourceId, null, patientIdentifier, null, null, null);

        // Performer
        //TODO Is IDENTIFIER_SYSTEM_BARTS_PERSONNEL_ID the right one?
        //  ResourceId clinicianResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getPatientId(), null, null,null, null, null, organisationResourceId, null, patientIdentifier, null, null, null);
        ResourceId clinicianResourceId = getResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, "Practitioner", parser.getClinicianID());

        // Encounter
        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterId());
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterId());
            createEncounter(parser.getCurrentState(), fhirResourceFiler, patientResourceId, null, encounterResourceId, Encounter.EncounterState.FINISHED, parser.getEffectiveDateTime(), parser.getEffectiveDateTime(), null, Encounter.EncounterClass.OTHER);
        }
        // this Observation resource id
        ResourceId observationResourceId = getObservationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getPatientId(), parser.getEffectiveDateTimeAsString(), parser.getEventCode());

        //Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT , "clinical coding")};

        String observationID = parser.getEventId();
        Observation fhirObservation = new Observation();
        fhirObservation.setId(observationResourceId.getResourceId().toString());
        fhirObservation.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_OBSERVATION));
        fhirObservation.addIdentifier().setSystem(FhirCodeUri.CODE_SYSTEM_CERNER_OBSERVATION_ID).setValue(observationID);
        fhirObservation.setSubject(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
        //TODO we need to filter out any records that are not final
        fhirObservation.setStatus(org.hl7.fhir.instance.model.Observation.ObservationStatus.FINAL);
        fhirObservation.addPerformer(ReferenceHelper.createReference(ResourceType.Practitioner, clinicianResourceId.getResourceId().toString()));

        Date effectiveDate = parser.getEffectiveDateTime();
        String effectiveDatePrecision = "YMD";
        fhirObservation.setEffective(EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision));

        fhirObservation.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));

//        String clinicianID = parser.getClinicianID();
//        if (!Strings.isNullOrEmpty(clinicianID)) {
//            //TODO - need to map to person resources
//            //fhirObservation.addPerformer(csvHelper.createPractitionerReference(clinicianID));
//        }

        String term = parser.getEventTitleText();
        if (Strings.isNullOrEmpty(term)) {
            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.CLINICAL_CODE_TYPE,
                    Long.parseLong(parser.getEventCode()),
                    fhirResourceFiler.getServiceId());
            if (cernerCodeValueRef != null) {
                term = cernerCodeValueRef.getCodeDispTxt();
            } else {
                // LOG.warn("Event type code: " + parser.getEventCode() + " not found in Code Value lookup");
            }



        }

        String code = parser.getEventCode();

        //TODO - establish code mapping for millenium / FHIR
        // Just staying with FHIR code for now - valid approach?
        CodeableConcept obsCode = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID, term, code);
        //TerminologyService.translateToSnomed(obsCode);
        // Previous null test wouldn't work. Specific errors should get an exception
        if (obsCode.getCoding().isEmpty()) {
            LOG.warn("Unable to create codeableConcept for Observation ID: " + observationID);
            return;
        } else {
            fhirObservation.setCode(obsCode);
        }

        //set value and units
        //TODO need to check getEventResultClassCode()
        // Need to store comparator separately  - new method in Quantityhelper
        // if it looks like a plain number it's simple.
        // if it contains  valid comparator symbol, use it
        String value = parser.getEventResultAsText();
        if (!Strings.isNullOrEmpty(value)) {
            Number num = NumberFormat.getInstance().parse(value);
            if (num == null) {
                //TODO we need to cater for some non-numeric results
                LOG.debug("Not proceeding with non-numeric value for now");
                return;
            }
            Double value1 = Double.parseDouble(value);
            String unitsCode = parser.getEventUnitsCode();
            String units = "";   //TODO - map units from unitsCode
            for (String comp : comparators) {
                if (value.contains(comp)) {
                    fhirObservation.setValue(QuantityHelper.createSimpleQuantity(value1,
                            units,
                            Quantity.QuantityComparator.fromCode(comp),
                            FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID,
                            unitsCode));
                } else {
                    fhirObservation.setValue(QuantityHelper.createSimpleQuantity(value1, units, FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID, unitsCode));
                }
            }

            //TODO - set comments
            fhirObservation.setComments(parser.getEventTag());

            // Reference range if supplied
            if (parser.getEventNormalRangeLow() != null && parser.getEventNormalRangeHigh() != null) {
                Double rangeLow = Double.parseDouble(parser.getEventNormalRangeLow());
                SimpleQuantity low = QuantityHelper.createSimpleQuantity(rangeLow, units);
                Double rangeHigh = Double.parseDouble(parser.getEventNormalRangeHigh());
                SimpleQuantity high = QuantityHelper.createSimpleQuantity(rangeHigh, units);
                //TODO I think I need a new createcodeableconcept method without term ???
                // I think the fhir page suggests this but may be wrong
                CodeableConcept normCode = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID,
                        "normal", parser.getEventNormalcyCode());
                Observation.ObservationReferenceRangeComponent obsRange = new Observation.ObservationReferenceRangeComponent();
                obsRange.setHigh(high);
                obsRange.setLow(low);
                obsRange.setMeaning(normCode);
                fhirObservation.addReferenceRange(obsRange);
            }
            //TODO save the cerner codes and somehow manage the parent id to make test results comprehensible
            // Looks like we have to correlate via encounter id for now


            // save resource
            if (parser.isActive()) {
                LOG.debug("Save Observation (PatId=" + parser.getPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirObservation));
                savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirObservation);
            } else {
                LOG.debug("Delete Observation (PatId=" + parser.getPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirObservation));
                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirObservation);
            }

        }

    }*/

}
