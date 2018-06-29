package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerClinicalEventMappingState;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.reference.CernerClinicalEventMappingDalI;
import org.endeavourhealth.core.database.dal.reference.SnomedDalI;
import org.endeavourhealth.core.database.dal.reference.models.CernerClinicalEventMap;
import org.endeavourhealth.core.database.dal.reference.models.SnomedLookup;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.CLEVE;
import org.endeavourhealth.transform.common.*;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CLEVEPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(CLEVEPreTransformer.class);

    private static CernerClinicalEventMappingDalI referenceDal = DalProvider.factoryCernerClinicalEventMappingDal();
    private static CernerCodeValueRefDalI cernerDal = DalProvider.factoryCernerCodeValueRefDal();
    private static SnomedDalI snomedDal = DalProvider.factorySnomedDal();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        //we need to write a lot of stuff to the DB and each record is independent, so use a thread pool to parallelise
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 10000);

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {
                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }
                    //no try/catch here, since any failure here means we don't want to continue
                    processRecord((CLEVE)parser, fhirResourceFiler, csvHelper, threadPool);
                }
            }
        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            AbstractCsvCallable.handleErrors(errors);
        }
    }


    public static void processRecord(CLEVE parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper, ThreadPool threadPool) throws Exception {

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

        PreTransformCallable callable = new PreTransformCallable(parser.getCurrentState(), eventIdCell, activeCell, parentEventIdCell,
                                                    encounterIdCell, eventCdCell, eventClassCdCell, eventResultUnitsCdCell,
                                                    eventResultTxtCell, resultValueCell, eventTitleCell, eventTagCell, csvHelper);
        List<ThreadPoolError> errors = threadPool.submit(callable);
        AbstractCsvCallable.handleErrors(errors);
    }

    static class PreTransformCallable extends AbstractCsvCallable {

        private CsvCell eventIdCell;
        private CsvCell activeCell;
        private CsvCell parentEventIdCell;
        private CsvCell encounterIdCell;
        private CsvCell eventCdCell;
        private CsvCell eventClassCdCell;
        private CsvCell eventResultUnitsCdCell;
        private CsvCell eventResultTxtCell;
        private CsvCell resultValueCell;
        private CsvCell eventTitleCell;
        private CsvCell eventTagCell;
        private BartsCsvHelper csvHelper;

        public PreTransformCallable(CsvCurrentState parserState, CsvCell eventIdCell, CsvCell activeCell, CsvCell parentEventIdCell, CsvCell encounterIdCell, CsvCell eventCdCell, CsvCell eventClassCdCell, CsvCell eventResultUnitsCdCell, CsvCell eventResultTxtCell, CsvCell resultValueCell, CsvCell eventTitleCell, CsvCell eventTagCell, BartsCsvHelper csvHelper) {
            super(parserState);
            this.eventIdCell = eventIdCell;
            this.activeCell = activeCell;
            this.parentEventIdCell = parentEventIdCell;
            this.encounterIdCell = encounterIdCell;
            this.eventCdCell = eventCdCell;
            this.eventClassCdCell = eventClassCdCell;
            this.eventResultUnitsCdCell = eventResultUnitsCdCell;
            this.eventResultTxtCell = eventResultTxtCell;
            this.resultValueCell = resultValueCell;
            this.eventTitleCell = eventTitleCell;
            this.eventTagCell = eventTagCell;
            this.csvHelper = csvHelper;
        }


        @Override
        public Object call() throws Exception {

            try {

                if (activeCell.getIntAsBoolean()) {
                    //if non-deleted

                    //cache our parent-child link
                    //group header CLEVE records have their own ID as their parent, so ignore those ones
                    if (!BartsCsvHelper.isEmptyOrIsZero(parentEventIdCell)
                            && !parentEventIdCell.equalsValue(eventIdCell)) {
                        csvHelper.cacheParentChildClinicalEventLink(eventIdCell, parentEventIdCell);
                    }

                    //cache our encounter link
                    csvHelper.cacheNewConsultationChildRelationship(encounterIdCell, eventIdCell, ResourceType.Observation);

                    //map to Snomed if possible
                    String snomedConceptId = null;

                    //currently, mapping only works for numerics
                    if (CLEVETransformer.isNumericResult(eventClassCdCell, resultValueCell, eventResultTxtCell, csvHelper)) {
                        CernerClinicalEventMap mapping = referenceDal.findMappingForCvrefCode(eventCdCell.getLong());
                        if (mapping != null) {
                            snomedConceptId = mapping.getSnomedConceptId();
                        }
                    }

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
                    if (!eventTitleCell.isEmpty()) {
                        mappingState.setEventTitleText(eventTitleCell.getString());
                    }
                    if (!eventTagCell.isEmpty()) {
                        mappingState.setEventTagText(eventTagCell.getString());
                    }
                    if (!Strings.isNullOrEmpty(snomedConceptId)) {
                        mappingState.setMappedSnomedId(snomedConceptId);
                    }

                    cernerDal.updateCleveMappingStateTable(mappingState);

                } else {
                    //if deleted

                    //delete from the CLEVE mapping table
                    CernerClinicalEventMappingState mappingState = new CernerClinicalEventMappingState();
                    mappingState.setServiceId(csvHelper.getServiceId());
                    mappingState.setEventId(eventIdCell.getLong()); //only need to populate these two columns for the delete

                    cernerDal.deleteCleveMappingStateTable(mappingState);
                }

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

}
