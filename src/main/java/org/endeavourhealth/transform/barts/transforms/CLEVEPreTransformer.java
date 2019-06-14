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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class CLEVEPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(CLEVEPreTransformer.class);

    private static StagingClinicalEventDalI repository = DalProvider.factoryBartsStagingClinicalEventDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            for (ParserI parser : parsers) {
                while (parser.nextRecord()) {

                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }

                    //no try/catch here, since any failure here means we don't want to continue
                    processRecord((CLEVE) parser, fhirResourceFiler, csvHelper);
                }
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }

    }

    public static void processRecord(CLEVE parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        StagingClinicalEvent stagingClinicalEvent = new StagingClinicalEvent();

        stagingClinicalEvent.setActiveInd(parser.getActiveIndicator().getIntAsBoolean());
        stagingClinicalEvent.setExchangeId(parser.getExchangeId().toString());
        stagingClinicalEvent.setDtReceived(csvHelper.getDataDate());

        CsvCell eventIdCell = parser.getEventId();
        stagingClinicalEvent.setEventId(eventIdCell.getLong());

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(eventIdCell.getPublishedFileId(), eventIdCell.getRecordNumber());
        stagingClinicalEvent.setAuditJson(audit);

        boolean activeInd = parser.getActiveIndicator().getIntAsBoolean();
        stagingClinicalEvent.setActiveInd(activeInd);

        //only set additional values if active
        if (activeInd) {

            Integer personId = parser.getPersonId().getInt();
            if (Strings.isNullOrEmpty(personId.toString())) {
                TransformWarnings.log(LOG, csvHelper, "No person ID found for CLEVE {}", eventIdCell);
                return;
            }

            stagingClinicalEvent.setPersonId(personId);

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
                stagingClinicalEvent.setOrderId(orderIdCell.getInt());
            }

            CsvCell parentEventIdCell = parser.getParentEventId();
            if (!parentEventIdCell.isEmpty()) {
                stagingClinicalEvent.setParentEventId(parentEventIdCell.getInt());
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

            Date procDate;

            CsvCell eventStartDateCell = parser.getEventStartDateTime();
            procDate = BartsCsvHelper.parseDate(eventStartDateCell);
            stagingClinicalEvent.setEventStartDtTm(procDate);

            CsvCell eventEndDateCell = parser.getEventEndDateTime();
            procDate = BartsCsvHelper.parseDate(eventEndDateCell);
            stagingClinicalEvent.setEventEndDtTm(procDate);

            CsvCell clinicalSignificantDateCell = parser.getClinicallySignificantDateTime();
            procDate = BartsCsvHelper.parseDate(clinicalSignificantDateCell);
            stagingClinicalEvent.setClinicallySignificantDtTm(procDate);

            CsvCell eventCodeClassCell = parser.getEventCodeClass();

            if (!eventCodeClassCell.isEmpty()) {
                stagingClinicalEvent.setEventClassCd(eventCodeClassCell.getInt());
                if (!BartsCsvHelper.isEmptyOrIsZero(eventCodeClassCell)) {
                    CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_CLASS, eventCodeClassCell);
                    if (codeRef != null) {
                        stagingClinicalEvent.setLookupEventClass(codeRef.getCodeDispTxt());
                    }
                }
            }

            CsvCell resultNormalcyCell = parser.getEventNormalcyCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(resultNormalcyCell)) {
                CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_NORMALCY, resultNormalcyCell);
                if (codeRef != null) {
                    String codeDesc = codeRef.getCodeDispTxt();
                    stagingClinicalEvent.setLookupNormalcy(codeDesc);

                }
            }

            CsvCell resultTextCell = parser.getEventResultText();
            if (!resultTextCell.isEmpty()) {
                stagingClinicalEvent.setEventResultTxt(resultTextCell.getString());
            }

            CsvCell resultNbrCell = parser.getEventResultNumber();
            if (!resultNbrCell.isEmpty()) {
                stagingClinicalEvent.setEventResultNbr(resultNbrCell.getInt());
            }

            CsvCell eventResultDateCell = parser.getEventResultDateTime();
            procDate = BartsCsvHelper.parseDate(eventResultDateCell);
            stagingClinicalEvent.setEventResultDt(procDate);

            CsvCell resultClassCode = parser.getEventResultStatusCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(resultClassCode)) {
                CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_STATUS, resultClassCode);
                if (codeRef != null) {
                    String codeDesc = codeRef.getCodeDispTxt();
                    stagingClinicalEvent.setLookupEventResultStatus(codeDesc);

                }
            }

            CsvCell normalRangeLowCell = parser.getEventNormalRangeLow();
            if (!normalRangeLowCell.isEmpty()) {
                stagingClinicalEvent.setNormalRangeLowTxt(normalRangeLowCell.getString());
            }

            CsvCell normalRangeHighCell = parser.getEventNormalRangeHigh();
            if (!normalRangeHighCell.isEmpty()) {
                stagingClinicalEvent.setNormalRangeHighTxt(normalRangeHighCell.getString());
            }

            CsvCell eventPerformedDateCell = parser.getEventPerformedDateTime();
            procDate = BartsCsvHelper.parseDate(eventPerformedDateCell);
            stagingClinicalEvent.setEventPerformedDtTm(procDate);

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

            CsvCell resultUnitsCodeCell = parser.getEventResultUnitsCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(resultUnitsCodeCell)) {
                stagingClinicalEvent.setEventResultUnitsCd(resultUnitsCodeCell.getInt());
                CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_UNITS, resultUnitsCodeCell);
                if (codeRef != null) {
                    String codeDesc = codeRef.getCodeDispTxt();
                    stagingClinicalEvent.setLookupEventResultsUnitsCode(codeDesc);

                }
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

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new CLEVEPreTransformer.saveDataCallable(stagingClinicalEvent, serviceId));
    }

    public static class saveDataCallable implements Callable {

        private StagingClinicalEvent obj = null;
        private UUID serviceId;

        public saveDataCallable(StagingClinicalEvent objs, UUID serviceId) {
            this.obj = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.save(obj, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}
