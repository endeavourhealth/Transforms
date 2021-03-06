package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingClinicalEventDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingClinicalEvent;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.CLEVE;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class CLEVEPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(CLEVEPreTransformer.class);

    private static StagingClinicalEventDalI repository = DalProvider.factoryBartsStagingClinicalEventDalI();

    private static final String[] comparators = {"<=", "<", ">=", ">"};


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        List<StagingClinicalEvent> batch = new ArrayList<>();

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {

                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
                    continue;
                }
                //no try/catch here, since any failure here means we don't want to continue
                processRecord((CLEVE) parser, fhirResourceFiler, csvHelper, batch);
            }
        }

        saveBatch(batch, true, csvHelper);

    }

    private static void saveBatch(List<StagingClinicalEvent> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new CLEVEPreTransformer.saveDataCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    public static void processRecord(CLEVE parser, FhirResourceFiler fhirResourceFiler,
                                     BartsCsvHelper csvHelper, List<StagingClinicalEvent> batch) throws Exception {

        StagingClinicalEvent stagingClinicalEvent = new StagingClinicalEvent();

        stagingClinicalEvent.setActiveInd(parser.getActiveIndicator().getIntAsBoolean());
        stagingClinicalEvent.setExchangeId(parser.getExchangeId().toString());
        stagingClinicalEvent.setDtReceived(csvHelper.getDataDate());

        CsvCell eventIdCell = parser.getEventId();
        stagingClinicalEvent.setEventId(eventIdCell.getLong().longValue());

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(eventIdCell.getPublishedFileId(), eventIdCell.getRecordNumber());
        stagingClinicalEvent.setAuditJson(audit);

        boolean activeInd = parser.getActiveIndicator().getIntAsBoolean();
        stagingClinicalEvent.setActiveInd(activeInd);

        //only set additional values if active
        if (activeInd) {

            // ignore non numeric events bt allow Covid results
            if (!isNumericResult(parser, csvHelper)
                    && !isMappedTextualResult(parser)) {
                return;
            }

            CsvCell eventCodeClassCell = parser.getEventCodeClass();

            if (!eventCodeClassCell.isEmpty()) {
                stagingClinicalEvent.setEventClassCd(eventCodeClassCell.getInt());
                if (!BartsCsvHelper.isEmptyOrIsZero(eventCodeClassCell)) {
                    CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_CLASS, eventCodeClassCell);
                    if (codeRef != null) {
                        stagingClinicalEvent.setLookupEventClass(codeRef.getCodeDescTxt());
                    }
                }
            }


            Integer personId = parser.getPersonId().getInt();
            if (Strings.isNullOrEmpty(personId.toString())) {
                TransformWarnings.log(LOG, csvHelper, "No person ID found for CLEVE {}", eventIdCell);
                return;
            }

            stagingClinicalEvent.setPersonId(personId.intValue());

            //TYPE_MILLENNIUM_PERSON_ID_TO_MRN
            String mrn = csvHelper.getInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personId.toString());
            if (mrn != null) {
                stagingClinicalEvent.setLookupMrn(mrn);
            } else {
                TransformWarnings.log(LOG, csvHelper, "CLEVE {} has no MRN from lookup for person {}", eventIdCell, personId);
            }

            //LOG.debug("Processing procedure " + procedureIdCell.getString());

            CsvCell encounterIdCell = parser.getEncounterId();
            stagingClinicalEvent.setEncounterId(encounterIdCell.getInt());

            /*//DAB-121 enhancement to derive the responsiblePersonnelId from the encounter internal map
            String responsiblePersonnelId = csvHelper.findResponsiblePersonnelIdFromEncounterId(encounterIdCell);
            if (!Strings.isNullOrEmpty(responsiblePersonnelId)) {
                stagingClinicalEvent.setLookupResponsiblePersonnelId(Integer.valueOf(responsiblePersonnelId));
            }*/

            CsvCell orderIdCell = parser.getOrderId();
            if (!orderIdCell.isEmpty()) {
                stagingClinicalEvent.setOrderId(orderIdCell.getLong());
            }

            CsvCell parentEventIdCell = parser.getParentEventId();
            if (!parentEventIdCell.isEmpty()) {
                stagingClinicalEvent.setParentEventId(parentEventIdCell.getLong());
            }

            CsvCell eventCdCell = parser.getEventCode();

            if (!eventCdCell.isEmpty()) {
                stagingClinicalEvent.setEventCd(eventCdCell.getString());
                if (!BartsCsvHelper.isEmptyOrIsZero(eventCdCell)) {
                    CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_CODE_TYPE, eventCdCell);
                    if (codeRef != null) {
                        stagingClinicalEvent.setLookupEventCode(codeRef.getCodeValueCd());
                        stagingClinicalEvent.setLookupEventTerm(codeRef.getCodeDispTxt());
                    }
                }
            }

            CsvCell eventStartDateCell = parser.getEventStartDateTime();
            Date startDate = BartsCsvHelper.parseDate(eventStartDateCell);
            stagingClinicalEvent.setEventStartDtTm(startDate);

            CsvCell eventEndDateCell = parser.getEventEndDateTime();
            Date endDate = BartsCsvHelper.parseDate(eventEndDateCell);
            stagingClinicalEvent.setEventEndDtTm(endDate);

            CsvCell clinicalSignificantDateCell = parser.getClinicallySignificantDateTime();
            Date clinicalSigDate = BartsCsvHelper.parseDate(clinicalSignificantDateCell);
            stagingClinicalEvent.setClinicallySignificantDtTm(clinicalSigDate);

            CsvCell resultNormalcyCell = parser.getEventNormalcyCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(resultNormalcyCell)) {
                stagingClinicalEvent.setNormalcyCd(resultNormalcyCell.getInt());
                CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_NORMALCY, resultNormalcyCell);
                if (codeRef != null) {
                    String codeDesc = codeRef.getCodeDispTxt();
                    stagingClinicalEvent.setLookupNormalcy(codeDesc);

                }
            }

            //carry over numeric result
            if (isNumericResult(parser, csvHelper)) {
                transformResultNumericFields(parser, stagingClinicalEvent, csvHelper);
            }

            //carry over textual result
            CsvCell resultTextCell = parser.getEventResultText();
            if (!resultTextCell.isEmpty()) {
                stagingClinicalEvent.setEventResultTxt(resultTextCell.getString());
            }

            //carry over date result
            CsvCell eventResultDateCell = parser.getEventResultDateTime();
            if (!eventResultDateCell.isEmpty()) {
                Date resultDate = BartsCsvHelper.parseDate(eventResultDateCell);
                stagingClinicalEvent.setEventResultDt(resultDate);
            }

            CsvCell resultClassCode = parser.getEventResultStatusCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(resultClassCode)) {
                stagingClinicalEvent.setEventResultStatusCd(resultClassCode.getInt());
                CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_STATUS, resultClassCode);
                if (codeRef != null) {
                    String codeDesc = codeRef.getCodeDispTxt();
                    stagingClinicalEvent.setLookupEventResultStatus(codeDesc);

                }
            }

            CsvCell eventPerformedDateCell = parser.getEventPerformedDateTime();
            Date performedDate = BartsCsvHelper.parseDate(eventPerformedDateCell);
            stagingClinicalEvent.setEventPerformedDtTm(performedDate);

            CsvCell eventPerformedPersonCell = parser.getEventPerformedPersonnelId();
            if (!eventPerformedPersonCell.isEmpty()) {
                stagingClinicalEvent.setEventPerformedPrsnlId(eventPerformedPersonCell.getInt());
            }

            CsvCell eventTagCell = parser.getEventTag();
            if (!eventTagCell.isEmpty()) {
                stagingClinicalEvent.setEventTag(eventTagCell.getString());
            }

            CsvCell eventTitleTextCell = parser.getEventTitleText();
            if (!eventTitleTextCell.isEmpty()) {
                stagingClinicalEvent.setEventTitleTxt(eventTitleTextCell.getString());
            }

            CsvCell recordStatusCodeCell = parser.getRecordStatusReference();
            if (!BartsCsvHelper.isEmptyOrIsZero(recordStatusCodeCell)) {
                stagingClinicalEvent.setRecordStatusCd(recordStatusCodeCell.getInt());
                CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_STATUS, recordStatusCodeCell);
                if (codeRef != null) {
                    String codeDesc = codeRef.getCodeDispTxt();
                    stagingClinicalEvent.setLookupRecordStatusCode(codeDesc);

                }
            }
        }

        batch.add(stagingClinicalEvent);
        saveBatch(batch, false, csvHelper);

    }

    private static boolean isNumericResult(CLEVE parser, BartsCsvHelper csvHelper) throws Exception {

        CsvCell classCell = parser.getEventCodeClass();
        CsvCell resultValueCell = parser.getEventResultNumber();
        CsvCell resultTextCell = parser.getEventResultText();

        return isNumericResult(classCell, resultValueCell, resultTextCell, csvHelper);
    }

    private static boolean isMappedTextualResult(CLEVE parser) throws Exception {
        CsvCell eventCodeCell = parser.getEventCode();
        String mappedSnomedConcept = IMHelper.getMappedSnomedConceptForSchemeCode(IMConstant.BARTS_CERNER, eventCodeCell.getString());
        if (!Strings.isNullOrEmpty(mappedSnomedConcept)) {
            LOG.debug("Passing through text result for CLEVE " + parser.getEventId().getString() + " with raw code [" + eventCodeCell.getString() + "] mapped to Snomed concept [" + mappedSnomedConcept + "]");
            return true;

        } else {
            return false;
        }
    }

    public static boolean isNumericResult(CsvCell classCell, CsvCell resultValueCell, CsvCell resultTextCell, BartsCsvHelper csvHelper) throws Exception {

        //check that the class confirms our numeric status
        CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_CLASS, classCell);
        String classDesc = codeRef.getCodeDescTxt();
        if (!classDesc.equalsIgnoreCase("Numeric")) {
            return false;
        }

        if (resultTextCell.isEmpty()) {
            return false;
        }

        //despite the event class saying "numeric" there are lots of events where the result is "negative" (e.g. pregnancy tests)
        //so we need to test the value itself can be turned into a number.  Also checks for NaN values which are NotANumber
        String resultText = resultTextCell.getString();
        try {
            Double value = new Double(resultText);
            if (value.isNaN())
                return false;
            else
                return true;

        } catch (NumberFormatException nfe) {
            //if it's not a number, try checking for comparators at the start
            //return false;
        }

        for (String comparator : comparators) {
            if (resultText.startsWith(comparator)) {

                //remove the comparator from the String, and tidy up any whitespace
                String test = resultText.substring(comparator.length());
                test = test.trim();

                try {
                    new Double(test);
                    return true;

                } catch (NumberFormatException nfe) {
                    continue;
                }
            }
        }

        return false;
    }

    private static void transformResultNumericFields(CLEVE parser, StagingClinicalEvent stagingClinicalEvent, BartsCsvHelper csvHelper) throws Exception {

        //numeric results have their number in both the result text column and the result number column
        //HOWEVER the result number column seems to round them to the nearest int, so it less useful. So
        //get the numeric values from the result text cell
        CsvCell resultTextCell = parser.getEventResultText();
        String resultText = resultTextCell.getString();

        for (String comparator : comparators) {
            if (resultText.startsWith(comparator)) {
                stagingClinicalEvent.setComparator(comparator);

                //make sure to remove the comparator from the String, and tidy up any whitespace
                resultText = resultText.substring(comparator.length());
                resultText = resultText.trim();
            }
        }

        //test if the remaining result text is a number, otherwise it could just have been free-text that started with a number
        try {
            //try treating it as a number
            Double valueNumber = new Double(resultText);
            stagingClinicalEvent.setProcessedNumericResult(valueNumber);

        } catch (NumberFormatException nfe) {
            // LOG.warn("Failed to convert [" + resultText + "] to Double");
            TransformWarnings.log(LOG, parser, "Failed to convert {} to Double", resultText);
        }

        CsvCell unitsCodeCell = parser.getEventResultUnitsCode();
        String unitsDesc = "";
        if (!BartsCsvHelper.isEmptyOrIsZero(unitsCodeCell)) {
            stagingClinicalEvent.setEventResultUnitsCd(unitsCodeCell.getInt());
            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_UNITS, unitsCodeCell);
            if (cernerCodeValueRef != null) {
                unitsDesc = cernerCodeValueRef.getCodeDispTxt();
                stagingClinicalEvent.setLookupEventResultsUnitsCode(unitsDesc);
            }
        }

        // Reference range if supplied
        CsvCell low = parser.getEventNormalRangeLow();
        CsvCell high = parser.getEventNormalRangeHigh();

        if (!low.isEmpty() || !high.isEmpty()) {
            //going by how lab results were defined in the pathology spec, if we have upper and lower bounds,
            //it's an inclusive range. If we only have one bound, then it's non-inclusive.

            //sometimes the brackets are passed down from the path system to Cerner so strip them off
            String lowParsed = low.getString().replace("(", "");
            String highParsed = high.getString().replace(")", "");

            try {
                if (!Strings.isNullOrEmpty(lowParsed)) {
                    stagingClinicalEvent.setNormalRangeLowTxt(low.getString());
                    stagingClinicalEvent.setNormalRangeLowValue(new Double(lowParsed));
                }

                if (!Strings.isNullOrEmpty(highParsed)) {
                    stagingClinicalEvent.setNormalRangeHighTxt(high.getString());
                    stagingClinicalEvent.setNormalRangeHighValue(new Double(lowParsed));
                }
            } catch (NumberFormatException ex) {
                // LOG.warn("Range not set for Clinical Event " + parser.getEventId().getString() + " due to invalid reference range");
                TransformWarnings.log(LOG, parser, "Range not set for clinical event due to invalid reference range. Id:{}", parser.getEventId().getString());

            }
        }
    }

    public static class saveDataCallable implements Callable {

        private List<StagingClinicalEvent> objs = null;
        private UUID serviceId;

        public saveDataCallable(List<StagingClinicalEvent> objs, UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveCLEVEs(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}
