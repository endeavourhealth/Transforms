package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.ENCNT;
import org.endeavourhealth.transform.common.*;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ENCNTPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ENCNTTransformer.class);

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
                    //no try/catch here, because any error means it's not safe to continue
                    processRecord((ENCNT)parser, fhirResourceFiler, csvHelper, threadPool);
                }
            }
        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            AbstractCsvCallable.handleErrors(errors);
        }

    }

    /**
     * this pre-transformer tries to match the Data Warehouse Encounters to the HL7 Receiver Encounters
     * which needs the MRN and VISIT ID
     */
    public static void processRecord(ENCNT parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper, ThreadPool threadPool) throws Exception {

        //in-active (i.e. deleted) rows don't have anything else but the ID, so we can't do anything with them
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            return;
        }

        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell episodeIdCell = parser.getEpisodeIdentifier();
        CsvCell finCell = parser.getMillenniumFinancialNumberIdentifier();
        CsvCell visitIdCell = parser.getVisitId();

        PreTransformCallable callable = new PreTransformCallable(parser.getCurrentState(), personIdCell, encounterIdCell, episodeIdCell, finCell, visitIdCell, csvHelper);

        List<ThreadPoolError> errors = threadPool.submit(callable);
        AbstractCsvCallable.handleErrors(errors);
    }


    static class PreTransformCallable extends AbstractCsvCallable {

        private CsvCell personIdCell;
        private CsvCell encounterIdCell;
        private CsvCell episodeIdCell;
        private CsvCell finCell;
        private CsvCell visitIdCell;
        private BartsCsvHelper csvHelper;

        public PreTransformCallable(CsvCurrentState parserState, CsvCell personIdCell, CsvCell encounterIdCell, CsvCell episodeIdCell, CsvCell finCell, CsvCell visitIdCell, BartsCsvHelper csvHelper) {
            super(parserState);
            this.personIdCell = personIdCell;
            this.encounterIdCell = encounterIdCell;
            this.episodeIdCell = episodeIdCell;
            this.finCell = finCell;
            this.visitIdCell = visitIdCell;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {

                //99%+ of ENCNT records have a VISIT ID, but some don't, so we can't use them
                //also, VISIT IDs starting "RES_" seem to be used as placeholders, across multiple patients
                if (!visitIdCell.isEmpty()
                        && !visitIdCell.getString().startsWith("RES_")) {

                    //the HL7 Receiver uses the MRN as part of the Encounter ID, so we need to look that up
                    String mrn = csvHelper.getInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personIdCell.getString());
                    if (!Strings.isNullOrEmpty(mrn)) {

                        //the Data Warehouse files all use PersonID as the unique local identifier for patients, but the
                        //ADT feed uses the MRN, so we need to ensure that the Discovery UUID is the same as used by the ADT feed
                        String localUniqueId = encounterIdCell.getString();
                        String hl7ReceiverUniqueId = "PIdAssAuth=" + BartsCsvToFhirTransformer.PRIMARY_ORG_HL7_OID + "-PatIdValue=" + mrn + "-EpIdTypeCode=VISITID-EpIdValue=" + visitIdCell.getString(); //this must match the HL7 Receiver
                        String hl7ReceiverScope = csvHelper.getHl7ReceiverScope();
                        csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.Encounter, localUniqueId, hl7ReceiverUniqueId, hl7ReceiverScope, true);
                    }
                }

                //don't create the EpisodeOfCare yet, as we don't want to create one for every ENCNT. Just
                //call this fn to set up the Episode ID and FIN -> UUID mappings, so they can be picked up when
                //we do process the OPATT, AEATT and IPEPI files
                csvHelper.getEpisodeOfCareCache().setUpEpisodeOfCareBuilderMappings(encounterIdCell, personIdCell, episodeIdCell, finCell, visitIdCell, csvHelper);


            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}
