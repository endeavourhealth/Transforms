package org.endeavourhealth.transform.nwlextracts.transforms;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.endeavourhealth.transform.nwlextracts.schema.ENCNT;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

public class ENCNTPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ENCNTPreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 NwlExtractsCsvHelper csvHelper) throws Exception {

        List<CacheENCNT> batch = new ArrayList<>();

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                //no try/catch here, because any error means it's not safe to continue
                processRecord((ENCNT)parser, fhirResourceFiler, csvHelper, batch);
            }
        }

        saveBatch(batch, true, csvHelper, fhirResourceFiler);
    }

    private static void saveBatch(List<CacheENCNT> batch, boolean lastOne, NwlExtractsCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        csvHelper.submitToThreadPool(new PreTransformCallable(new ArrayList<>(batch), csvHelper, fhirResourceFiler));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    /**
     * this pre-transformer tries to match the Data Warehouse Encounters to the HL7 Receiver Encounters
     * which needs the MRN and VISIT ID
     */
    public static void processRecord(ENCNT parser, FhirResourceFiler fhirResourceFiler, NwlExtractsCsvHelper csvHelper, List<CacheENCNT> batch) throws Exception {


        //in-active (i.e. deleted) rows don't have anything else but the ID, so we can't do anything with them
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            return;
        }

        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell responsiblePersonnelIdCell = parser.getResponsibleHealthCareprovidingPersonnelIdentifier();
        CsvCell episodeIdCell = parser.getEpisodeIdentifier();
        CsvCell finCell = parser.getMillenniumFinancialNumberIdentifier();
        CsvCell visitIdCell = parser.getVisitId();

        CacheENCNT o = new CacheENCNT(personIdCell, encounterIdCell, responsiblePersonnelIdCell, episodeIdCell, finCell, visitIdCell);
        batch.add(o);
        saveBatch(batch, false, csvHelper, fhirResourceFiler);
    }

    static class CacheENCNT {
        private CsvCell personIdCell;
        private CsvCell encounterIdCell;
        private CsvCell responsiblePersonnelIdCell;
        private CsvCell episodeIdCell;
        private CsvCell finCell;
        private CsvCell visitIdCell;

        public CacheENCNT(CsvCell personIdCell, CsvCell encounterIdCell, CsvCell responsiblePersonnelIdCell, CsvCell episodeIdCell, CsvCell finCell, CsvCell visitIdCell) {
            this.personIdCell = personIdCell;
            this.encounterIdCell = encounterIdCell;
            this.responsiblePersonnelIdCell = responsiblePersonnelIdCell;
            this.episodeIdCell = episodeIdCell;
            this.finCell = finCell;
            this.visitIdCell = visitIdCell;
        }

        public String getPersonId() {
            return personIdCell.getString();
        }

        public String getEncounterId() {
            return encounterIdCell.getString();
        }

        public String getVisitId() {
            return visitIdCell.getString();
        }

        public CsvCell getPersonIdCell() {
            return personIdCell;
        }

        public CsvCell getEncounterIdCell() {
            return encounterIdCell;
        }

        public CsvCell getResponsiblePersonnelIdCell() {
            return responsiblePersonnelIdCell;
        }

        public CsvCell getEpisodeIdCell() {
            return episodeIdCell;
        }

        public CsvCell getFinCell() {
            return finCell;
        }

        public CsvCell getVisitIdCell() {
            return visitIdCell;
        }
    }

    static class PreTransformCallable implements Callable {

        private List<CacheENCNT> batch;
        private NwlExtractsCsvHelper csvHelper;
        private FhirResourceFiler fhirResourceFiler;

        public PreTransformCallable(List<CacheENCNT> batch,
                                    NwlExtractsCsvHelper csvHelper,
                                    FhirResourceFiler fhirResourceFiler) {
            this.batch = batch;
            this.csvHelper = csvHelper;
            this.fhirResourceFiler = fhirResourceFiler;
        }

        @Override
        public Object call() throws Exception {

            try {

                checkAndCreateEncounterUuids();

                checkAndCreateEpisodeOfCareUuids();


                //don't create the EpisodeOfCare yet, as we don't want to create one for every ENCNT. Just
                //call this fn to set up the Episode ID and FIN -> UUID mappings, so they can be picked up when
                //we do process the CDS encounter files to fall back on if EpisodeId is not provided

               /* //TODO - remove live check after test
                if (!TransformConfig.instance().isLive()) {
                    for (CacheENCNT o : batch) {
                        csvHelper.getEpisodeOfCareCache().setUpEpisodeOfCareBuilderMappings(o.encounterIdCell, o.personIdCell, o.episodeIdCell, o.finCell, o.visitIdCell);
                    }
                }*/

            }catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }

        private void checkAndCreateEncounterUuids() throws Exception {

            //generate UUIDs for each encounter, copying from HL7 Receiver where possible

            //first do a bulk check to see which encounters have UUID already assigned
            Set<ArrayList<Reference>> encounterReferences = new HashSet<>();
            Map<CacheENCNT, ArrayList<Reference>> hmReferences = new HashMap<>();

            for (CacheENCNT o : batch) {

                //99%+ of ENCNT records have a VISIT ID, but a tiny number don't, so we can't match them to the HL7 Receiver
                //also, VISIT IDs starting "RES_" seem to be used as placeholders, across multiple patients
                if (o.getVisitIdCell().isEmpty()
                        || o.getVisitId().startsWith("RES_")) {
                    continue;
                }

                Reference encounterIdReference = ReferenceHelper.createReference(ResourceType.Encounter, o.getEncounterId());
                Reference visitIdReference = ReferenceHelper.createReference(ResourceType.Encounter, o.getVisitId());
                ArrayList<Reference> referenceList = new ArrayList<Reference>();
                referenceList.add(0,encounterIdReference);
                referenceList.add(1,visitIdReference);
                encounterReferences.add(referenceList);

                hmReferences.put(o, referenceList); //this map lets us look up the references again
            }

            Map<Reference, UUID> hmExistingUuids = IdHelper.getEdsResourceIds(csvHelper.getServiceId(), encounterReferences, true);

            List<CacheENCNT> encountersWithoutUuids = new ArrayList<>();
            Set<String> hsPersonIds = new HashSet<>();

            for (CacheENCNT o : batch) {

                ArrayList<Reference> referenceList = hmReferences.get(o);

                //if no source reference, then encounter doesn't have a VISIT ID, so we can't try to copy from HL7 Receiver
                if (referenceList == null) {
                    continue;
                }

                //if we already have a UUID for our encounter, then skip it
                UUID existingMappedUuid = hmExistingUuids.get(referenceList.get(0));
                if (existingMappedUuid != null) {
                    continue;
                }

                encountersWithoutUuids.add(o);
                hsPersonIds.add(o.getPersonId());
            }

            //if we have any encounters without UUIDs, we should try to create them
            if (!encountersWithoutUuids.isEmpty()) {
                for (CacheENCNT o: encountersWithoutUuids) {
                    ArrayList<String> localUniqueIds = new ArrayList<String>();
                    localUniqueIds.add(0,o.getEncounterId());
                    localUniqueIds.add(1, o.getVisitId());
                    UUID edsId = csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.Encounter, localUniqueIds);
                }
            }
        }
        private void checkAndCreateEpisodeOfCareUuids() throws Exception {

            //generate UUIDs for each episode ofcare, copying from HL7 Receiver where possible

            //first do a bulk check to see which Episodes have UUID already assigned
            Set<ArrayList<Reference>> episodeOfCareReferences = new HashSet<>();
            Map<CacheENCNT, ArrayList<Reference>> hmReferences = new HashMap<>();

            for (CacheENCNT o : batch) {

                if (o.getVisitIdCell().isEmpty()
                        || o.getVisitId().startsWith("RES_")) {
                    continue;
                }

                Reference episodeOfCareReference = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, o.getEncounterId());
                Reference visitIdRefEpisode = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, o.getVisitId());
                ArrayList<Reference> referenceList = new ArrayList<Reference>();
                referenceList.add(0,episodeOfCareReference);
                referenceList.add(1,visitIdRefEpisode);
                episodeOfCareReferences.add(referenceList);

                hmReferences.put(o, referenceList); //this map lets us look up the references again
            }

            Map<Reference, UUID> hmExistingUuids = IdHelper.getEdsResourceIds(csvHelper.getServiceId(), episodeOfCareReferences, true);

            List<CacheENCNT> episodesWithoutUuids = new ArrayList<>();
            Set<String> hsPersonIds = new HashSet<>();

            for (CacheENCNT o : batch) {

                ArrayList<Reference> referenceList = hmReferences.get(o);

                //if no source reference, then encounter doesn't have a VISIT ID, so we can't try to copy from HL7 Receiver
                if (referenceList == null) {
                    continue;
                }

                //if we already have a UUID for our encounter, then skip it
                UUID existingMappedUuid = hmExistingUuids.get(referenceList.get(0));
                if (existingMappedUuid != null) {
                    continue;
                }

                episodesWithoutUuids.add(o);
                hsPersonIds.add(o.getPersonId());
            }

            //if we have any encounters without UUIDs, we should try to create them
            if (!episodesWithoutUuids.isEmpty()) {
                for (CacheENCNT o: episodesWithoutUuids) {
                    ArrayList<String> localUniqueIds = new ArrayList<String>();
                    localUniqueIds.add(0,o.getEncounterId());
                    localUniqueIds.add(1, o.getVisitId());
                    UUID edsId = csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.EpisodeOfCare, localUniqueIds);
                }
            }
        }

    }
}
