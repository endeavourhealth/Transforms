package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerClinicalEventMappingState;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.reference.CernerClinicalEventMappingDalI;
import org.endeavourhealth.core.database.dal.reference.SnomedDalI;
import org.endeavourhealth.core.database.dal.reference.models.CernerClinicalEventMap;
import org.endeavourhealth.core.database.dal.reference.models.SnomedLookup;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.CLEVE;
import org.endeavourhealth.transform.common.*;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class CLEVEPreTransformerOLD {
    private static final Logger LOG = LoggerFactory.getLogger(CLEVEPreTransformerOLD.class);

    private static CernerClinicalEventMappingDalI referenceDal = DalProvider.factoryCernerClinicalEventMappingDal();
    private static CernerCodeValueRefDalI cernerDal = DalProvider.factoryCernerCodeValueRefDal();
    private static SnomedDalI snomedDal = DalProvider.factorySnomedDal();
    private static Map<Long, CernerClinicalEventMap> cachedMappings = new HashMap<>(); //can't use concurrent hasmap, as that doesn't allow null values

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            List<CleveRecord> pendingUpdates = new ArrayList<>();
            List<CleveRecord> pendingDeletes = new ArrayList<>();

            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {
                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }
                    //no try/catch here, since any failure here means we don't want to continue
                    processRecord((CLEVE)parser, fhirResourceFiler, csvHelper, pendingUpdates, pendingDeletes);
                }
            }

            //make sure to run any pending ones left over
            if (!pendingUpdates.isEmpty()) {
                runPending(pendingUpdates, false, csvHelper);
            }
            if (!pendingDeletes.isEmpty()) {
                runPending(pendingDeletes, true, csvHelper);
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }


    public static void processRecord(CLEVE parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper, List<CleveRecord> pendingUpdates, List<CleveRecord> pendingDeletes) throws Exception {

        //observations have bi-directional child-parent links. We can create the child-to-parent link when processing
        //the CLEVE file normally, but need to pre-cache the links in order to create the parent-to-child ones
        //We also need to update the CLEVE mapping table, so may as well do here rather than when we're actually
        //creating the resource
        CsvCell eventIdCell = parser.getEventId();
        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell parentEventIdCell = parser.getParentEventId();
        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell eventCdCell = parser.getEventCode();
        CsvCell eventClassCdCell = parser.getEventCodeClass();
        CsvCell eventResultUnitsCdCell = parser.getEventResultUnitsCode();
        CsvCell eventResultTxtCell = parser.getEventResultText();
        CsvCell resultValueCell = parser.getEventResultNumber();
        CsvCell eventTitleCell = parser.getEventTitleText();
        CsvCell eventTagCell = parser.getEventTag();

        CleveRecord record = new CleveRecord(parser.getCurrentState(), eventIdCell, parentEventIdCell,
                encounterIdCell, eventCdCell, eventClassCdCell, eventResultUnitsCdCell,
                eventResultTxtCell, resultValueCell, eventTitleCell, eventTagCell);

        if (activeCell.getIntAsBoolean()) {
            pendingUpdates.add(record);
            if (pendingUpdates.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
                runPending(pendingUpdates, false, csvHelper);
            }

        } else {
            pendingDeletes.add(record);

            if (pendingDeletes.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
                runPending(pendingDeletes, true, csvHelper);
            }
        }
    }

    private static void runPending(List<CleveRecord> records, boolean isDelete, BartsCsvHelper csvHelper) throws Exception {

        List<CleveRecord> copy = new ArrayList<>(records);
        records.clear();

        PreTransformCallable callable = new PreTransformCallable(copy, isDelete, csvHelper);
        csvHelper.submitToThreadPool(callable);
    }

    public static boolean shouldTransformOrAuditRecord(CLEVE cleveParser, BartsCsvHelper csvHelper) throws Exception {

        CsvCell eventCdCell = cleveParser.getEventCode();
        CsvCell eventClassCdCell = cleveParser.getEventCodeClass();
        CsvCell eventResultTxtCell = cleveParser.getEventResultText();
        CsvCell resultValueCell = cleveParser.getEventResultNumber();

        if (eventCdCell.isEmpty()) {
            return false;
        }

        String snomedConceptId = mapToSnomedCode(eventClassCdCell, resultValueCell, eventResultTxtCell, eventCdCell, csvHelper);

        //if we can't map to Snomed, we won't create a FHIR resource, so skip auditing this record
        if (Strings.isNullOrEmpty(snomedConceptId)) {
            return false;
        }

        return true;
    }

    public static String mapToSnomedCode(CsvCell eventClassCdCell, CsvCell resultValueCell,
                                         CsvCell eventResultTxtCell, CsvCell eventCdCell,
                                         BartsCsvHelper csvHelper) throws Exception {

        //currently, mapping only works for numerics
        if (CLEVETransformerOLD.isNumericResult(eventClassCdCell, resultValueCell, eventResultTxtCell, csvHelper)) {

            //see if we've mapped this numeric to snomed
            Long key = eventCdCell.getLong();
            CernerClinicalEventMap mapping = null;
            synchronized (cachedMappings) {
                //slightly odd pattern because not all codes are mapped to snomed, so we'll have
                //null mappings that we want to cache in the map, so we don't keep perfomring the same DB hit for ones that aren't mapped
                if (cachedMappings.containsKey(key)) {
                    mapping = cachedMappings.get(key);

                } else {
                    mapping = referenceDal.findMappingForCvrefCode(eventCdCell.getLong());
                    cachedMappings.put(key, mapping);
                }
            }

            if (mapping != null) {
                return mapping.getSnomedConceptId();
            }
        }

        return null;
    }

    //simple storage class to carry over everything we need from the CSV parser so we can use in the thread pool
    static class CleveRecord {

        private CsvCurrentState parserState;
        private CsvCell eventIdCell;
        private CsvCell parentEventIdCell;
        private CsvCell encounterIdCell;
        private CsvCell eventCdCell;
        private CsvCell eventClassCdCell;
        private CsvCell eventResultUnitsCdCell;
        private CsvCell eventResultTxtCell;
        private CsvCell resultValueCell;
        private CsvCell eventTitleCell;
        private CsvCell eventTagCell;

        public CleveRecord(CsvCurrentState parserState, CsvCell eventIdCell, CsvCell parentEventIdCell, CsvCell encounterIdCell, CsvCell eventCdCell, CsvCell eventClassCdCell, CsvCell eventResultUnitsCdCell, CsvCell eventResultTxtCell, CsvCell resultValueCell, CsvCell eventTitleCell, CsvCell eventTagCell) {
            this.parserState = parserState;
            this.eventIdCell = eventIdCell;
            this.parentEventIdCell = parentEventIdCell;
            this.encounterIdCell = encounterIdCell;
            this.eventCdCell = eventCdCell;
            this.eventClassCdCell = eventClassCdCell;
            this.eventResultUnitsCdCell = eventResultUnitsCdCell;
            this.eventResultTxtCell = eventResultTxtCell;
            this.resultValueCell = resultValueCell;
            this.eventTitleCell = eventTitleCell;
            this.eventTagCell = eventTagCell;
        }

        public CsvCurrentState getParserState() {
            return parserState;
        }

        public CsvCell getEventIdCell() {
            return eventIdCell;
        }

        public CsvCell getParentEventIdCell() {
            return parentEventIdCell;
        }

        public CsvCell getEncounterIdCell() {
            return encounterIdCell;
        }

        public CsvCell getEventCdCell() {
            return eventCdCell;
        }

        public CsvCell getEventClassCdCell() {
            return eventClassCdCell;
        }

        public CsvCell getEventResultUnitsCdCell() {
            return eventResultUnitsCdCell;
        }

        public CsvCell getEventResultTxtCell() {
            return eventResultTxtCell;
        }

        public CsvCell getResultValueCell() {
            return resultValueCell;
        }

        public CsvCell getEventTitleCell() {
            return eventTitleCell;
        }

        public CsvCell getEventTagCell() {
            return eventTagCell;
        }
    }

    static class PreTransformCallable implements Callable {

        private List<CleveRecord> records;
        private boolean isDelete;
        private BartsCsvHelper csvHelper;

        public PreTransformCallable(List<CleveRecord> records, boolean isDelete, BartsCsvHelper csvHelper) {

            this.records = records;
            this.isDelete = isDelete;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {

                if (isDelete) {

                    //delete from the CLEVE mapping table
                    List<CernerClinicalEventMappingState> mappings = new ArrayList<>();
                    for (CleveRecord record: records) {

                        CernerClinicalEventMappingState mappingState = new CernerClinicalEventMappingState();
                        mappingState.setServiceId(csvHelper.getServiceId());
                        mappingState.setEventId(record.getEventIdCell().getLong()); //only need to populate these two columns for the delete

                        mappings.add(mappingState);
                    }

                    cernerDal.deleteCleveMappingStateTable(mappings);

                } else {

                    List<CernerClinicalEventMappingState> mappings = new ArrayList<>();

                    for (CleveRecord record: records) {
                        //cache our parent-child link
                        //group header CLEVE records have their own ID as their parent, so ignore those ones
                        CsvCell parentEventIdCell = record.getParentEventIdCell();
                        CsvCell eventIdCell = record.getEventIdCell();
                        if (!BartsCsvHelper.isEmptyOrIsZero(parentEventIdCell)
                                && !parentEventIdCell.equalsValue(eventIdCell)) {
                            csvHelper.cacheParentChildClinicalEventLink(eventIdCell, parentEventIdCell);
                        }

                        //cache our encounter link
                        CsvCell encounterIdCell = record.getEncounterIdCell();
                        csvHelper.cacheNewConsultationChildRelationship(encounterIdCell, eventIdCell, ResourceType.Observation);

                        CsvCell eventClassCdCell = record.getEventClassCdCell();
                        CsvCell resultValueCell = record.getResultValueCell();
                        CsvCell eventResultTxtCell = record.getEventResultTxtCell();
                        CsvCell eventCdCell = record.getEventCdCell();

                        //map to Snomed if possible
                        String snomedConceptId = CLEVEPreTransformerOLD.mapToSnomedCode(eventClassCdCell, resultValueCell, eventResultTxtCell, eventCdCell, csvHelper);

                        if (!Strings.isNullOrEmpty(snomedConceptId)) {
                            SnomedLookup snomedLookup = snomedDal.getSnomedLookup(snomedConceptId);
                            if (snomedLookup == null) {
                                throw new TransformException("Failed to find snomed_lookup for concept ID " + snomedConceptId);
                            }
                            csvHelper.cacheCleveSnomedConceptId(eventIdCell, snomedLookup);
                        }

                        //update the CLEVE mapping table
                        CernerClinicalEventMappingState mappingState = new CernerClinicalEventMappingState();
                        mappingState.setServiceId(csvHelper.getServiceId());
                        mappingState.setEventId(eventIdCell.getLong());
                        if (!BartsCsvHelper.isEmptyOrIsZero(eventCdCell)) {
                            mappingState.setEventCd(eventCdCell.getString());

                            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_CODE_TYPE, eventCdCell);
                            if (codeRef != null) {
                                mappingState.setEventCdTerm(codeRef.getCodeDispTxt());
                            }
                        }
                        if (!BartsCsvHelper.isEmptyOrIsZero(eventClassCdCell)) {
                            mappingState.setEventClassCd(eventClassCdCell.getString());

                            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_CLASS, eventClassCdCell);
                            if (codeRef != null) {
                                mappingState.setEventClassCdTerm(codeRef.getCodeDispTxt());
                            }
                        }
                        CsvCell eventResultUnitsCdCell = record.getEventResultUnitsCdCell();
                        if (!BartsCsvHelper.isEmptyOrIsZero(eventResultUnitsCdCell)) {
                            mappingState.setEventResultUnitsCd(eventResultUnitsCdCell.getString());

                            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_UNITS, eventResultUnitsCdCell);
                            if (codeRef != null) {
                                mappingState.setEventResultUnitsCdTerm(codeRef.getCodeDispTxt());
                            }
                        }
                        if (!eventResultTxtCell.isEmpty()) {
                            mappingState.setEventResultText(eventResultTxtCell.getString());
                        }
                        CsvCell eventTitleCell = record.getEventTitleCell();
                        if (!eventTitleCell.isEmpty()) {
                            mappingState.setEventTitleText(eventTitleCell.getString());
                        }
                        CsvCell eventTagCell = record.getEventTagCell();
                        if (!eventTagCell.isEmpty()) {
                            mappingState.setEventTagText(eventTagCell.getString());
                        }
                        if (!Strings.isNullOrEmpty(snomedConceptId)) {
                            mappingState.setMappedSnomedId(snomedConceptId);
                        }

                        mappings.add(mappingState);
                    }

                    cernerDal.updateCleveMappingStateTable(mappings);
                }

            } catch (Throwable t) {
                String msg = "Error with CLEVE pre-transform for records: ";
                for (CleveRecord record: records) {
                    msg += record.getParserState().toString();
                }
                LOG.error(msg, t);
                throw new TransformException(msg, t);
            }

            return null;
        }
    }

}
