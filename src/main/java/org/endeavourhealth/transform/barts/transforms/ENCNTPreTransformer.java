package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingPROCE;
import org.endeavourhealth.core.database.dal.publisherTransform.ResourceIdTransformDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.ENCNT;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

public class ENCNTPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ENCNTTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

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

    private static void saveBatch(List<CacheENCNT> batch, boolean lastOne, BartsCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

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
    public static void processRecord(ENCNT parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper, List<CacheENCNT> batch) throws Exception {

        //nasty hack to get through the 16M record bulk without
        /*String file = parser.getFilePath();
        if (file.contains("2017-12-03")
                && parser.getCurrentLineNumber() < 14000000L) {
            return;
        }*/

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
        private BartsCsvHelper csvHelper;
        private FhirResourceFiler fhirResourceFiler;

        public PreTransformCallable(List<CacheENCNT> batch,
                                    BartsCsvHelper csvHelper,
                                    FhirResourceFiler fhirResourceFiler) {
            this.batch = batch;
            this.csvHelper = csvHelper;
            this.fhirResourceFiler = fhirResourceFiler;
        }

        @Override
        public Object call() throws Exception {

            try {

                checkAndSavePersonIds();
                checkAndSaveConsultantIds();
                checkAndCreateEncounterUuids();


                //don't create the EpisodeOfCare yet, as we don't want to create one for every ENCNT. Just
                //call this fn to set up the Episode ID and FIN -> UUID mappings, so they can be picked up when
                //we do process the OPATT, AEATT and IPEPI files
//TODO - taken this out to get the procedures stuff loaded faster, as it depends on the above but not this
//                csvHelper.getEpisodeOfCareCache().setUpEpisodeOfCareBuilderMappings(encounterIdCell, personIdCell, episodeIdCell, finCell, visitIdCell);


            }catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }

        private void checkAndCreateEncounterUuids() throws Exception {

            //generate UUIDs for each encounter, copying from HL7 Receiver where possible

            //first do a bulk check to see which encounters have UUID already assigned
            Set<String> encounterIds = new HashSet<>();
            for (CacheENCNT o : batch) {

                //99%+ of ENCNT records have a VISIT ID, but a tiny number don't, so we can't match them to the HL7 Receiver
                //also, VISIT IDs starting "RES_" seem to be used as placeholders, across multiple patients
                if (o.getVisitIdCell().isEmpty()
                        || o.getVisitId().startsWith("RES_")) {
                    continue;
                }

                encounterIds.add(o.getEncounterId());
            }

            Map<String, UUID> hmExistingUuids = IdHelper.getEdsResourceIds(csvHelper.getServiceId(), ResourceType.Encounter, encounterIds);


            List<CacheENCNT> encountersWithoutUuids = new ArrayList<>();
            Set<String> hsPersonIds = new HashSet<>();

            for (CacheENCNT o : batch) {

                //if no source reference, then encounter doesn't have a VISIT ID, so we can't try to copy from HL7 Receiver
                if (!encounterIds.contains(o.getEncounterId())) {
                    continue;
                }

                //if we already have a UUID for our encounter, then skip it
                UUID existingMappedUuid = hmExistingUuids.get(o.getEncounterId());
                if (existingMappedUuid != null) {
                    continue;
                }

                encountersWithoutUuids.add(o);
                hsPersonIds.add(o.getPersonId());
            }

            //if we have any encounters without UUIDs, we should try to create them
            if (!encountersWithoutUuids.isEmpty()) {

                //find the MRN for each patient
                Map<String, String> hmExistingMrns = csvHelper.getInternalIds(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, hsPersonIds);

                for (CacheENCNT o: encountersWithoutUuids) {
                    String personId = o.getPersonId();
                    String mrn = hmExistingMrns.get(personId);

                    //if we've not saved an MRN for this person, then we can't copy from HL7 Receiver
                    if (mrn == null) {
                        continue;
                    }

                    //the Data Warehouse files all use PersonID as the unique local identifier for patients, but the
                    //ADT feed uses the MRN, so we need to ensure that the Discovery UUID is the same as used by the ADT feed
                    String localUniqueId = o.getEncounterId();
                    String hl7ReceiverUniqueId = "PIdAssAuth=" + BartsCsvToFhirTransformer.PRIMARY_ORG_HL7_OID + "-PatIdValue=" + mrn + "-EpIdTypeCode=VISITID-EpIdValue=" + o.getVisitId(); //this must match the HL7 Receiver
                    String hl7ReceiverScope = csvHelper.getHl7ReceiverScope();
                    csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.Encounter, localUniqueId, hl7ReceiverUniqueId, hl7ReceiverScope, true);
                }
            }
        }

        private void checkAndSaveConsultantIds() throws Exception {

            Set<String> encounterIds = new HashSet<>();
            for (CacheENCNT o : batch) {
                encounterIds.add(o.getEncounterId());
            }

            //store our mapped responsible person IDs
            //update the internal ID map with our encounter to responsible personnel mapping - DAB-121 enhancement
            Map<String, String> hmExistingPersonnelIds = csvHelper.getInternalIds(BartsCsvHelper.ENCOUNTER_ID_TO_RESPONSIBLE_PESONNEL_ID, encounterIds);

            for (CacheENCNT o : batch) {
                String existingPersonnelId = hmExistingPersonnelIds.get(o.getEncounterId());

                if (Strings.isNullOrEmpty(existingPersonnelId)
                        || !existingPersonnelId.equals(o.getResponsiblePersonnelIdCell().getString())) {
                    csvHelper.saveEncounterIdToResponsiblePersonnelId(o.getEncounterIdCell(), o.getResponsiblePersonnelIdCell());
                }
            }
        }


        private void checkAndSavePersonIds() throws Exception {

            Set<String> encounterIds = new HashSet<>();
            for (CacheENCNT o : batch) {
                encounterIds.add(o.getEncounterId());
            }

            //ensure our mapped person ID hasn't changed and store it
            Map<String, String> hmExistingPersonIds = csvHelper.getInternalIds(BartsCsvHelper.ENCOUNTER_ID_TO_PERSON_ID, encounterIds);

            for (CacheENCNT o : batch) {
                String existingPersonId = hmExistingPersonIds.get(o.getEncounterId());

                //if the person ID has changed, then we need to move stuff
                if (!Strings.isNullOrEmpty(existingPersonId)
                        && !existingPersonId.equals(o.getPersonId())) {

                    moveEncounterDependentData(existingPersonId, o.getPersonIdCell(), o.getEncounterIdCell());
                }

                //update the internal ID map with our encounter to person mapping
                if (Strings.isNullOrEmpty(existingPersonId)
                        || !existingPersonId.equals(o.getPersonId())) {
                    csvHelper.saveEncounterIdToPersonId(o.getEncounterIdCell(), o.getPersonIdCell());
                }
            }
        }



        /**
         * if an ENCNT record is moved from on person record to another, we need to manually move any dependent
         * resources (e.g. procedures)
         */
        private void moveEncounterDependentData(String existingPersonId, CsvCell personIdCell, CsvCell encounterIdCell) throws Exception {
            UUID patientUuid = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.Patient, existingPersonId);
            if (patientUuid == null) {
                return;
            }

            UUID newPatientUuid = IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.Patient, personIdCell.getString());
            Reference newPatientReference = ReferenceHelper.createReference(ResourceType.Patient, newPatientUuid.toString());

            UUID encounterUuid = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.Encounter, encounterIdCell.getString());
            if (encounterUuid == null) {
                return;
            }
            String encounterReferenceValue = ReferenceHelper.createResourceReference(ResourceType.Encounter, encounterUuid.toString());

            ResourceDalI resourceDal = DalProvider.factoryResourceDal();
            List<ResourceWrapper> resources = resourceDal.getResourcesByPatient(csvHelper.getServiceId(), patientUuid);
            for (ResourceWrapper resourceWrapper: resources) {
                Resource resource = FhirSerializationHelper.deserializeResource(resourceWrapper.getResourceData());

                if (resource.getResourceType() == ResourceType.Patient
                        || resource.getResourceType() == ResourceType.Observation
                        || resource.getResourceType() == ResourceType.EpisodeOfCare) {
                    //ignore these because they're not dependent on encounters

                } else if (resource.getResourceType() == ResourceType.Encounter) {
                    Encounter encounter = (Encounter)resource;
                    if (encounter.getId().equals(encounterUuid.toString())) {
                        EncounterBuilder builder = new EncounterBuilder(encounter);
                        builder.setPatient(newPatientReference);
                        fhirResourceFiler.savePatientResource(null, false, builder);
                    }

                } else if (resource.getResourceType() == ResourceType.Procedure) {
                    Procedure procedure = (Procedure)resource;
                    if (procedure.hasEncounter()
                            && procedure.getEncounter().getReference().equals(encounterReferenceValue)) {
                        ProcedureBuilder builder = new ProcedureBuilder(procedure);
                        builder.setPatient(newPatientReference);
                        fhirResourceFiler.savePatientResource(null, false, builder);
                    }

                } else if (resource.getResourceType() == ResourceType.Condition) {
                    Condition condition = (Condition)resource;
                    if (condition.hasEncounter()
                            && condition.getEncounter().getReference().equals(encounterReferenceValue)) {
                        ConditionBuilder builder = new ConditionBuilder(condition);
                        builder.setPatient(newPatientReference);
                        fhirResourceFiler.savePatientResource(null, false, builder);
                    }

                }  else {
                    //most resources haven't had the right stuff above implemented, so
                    throw new Exception("Unhandled resource type " + resource.getResourceType() + " when moving encounter to another patient " + existingPersonId + " -> " + personIdCell.getString());
                }
            }
        }
    }
}
