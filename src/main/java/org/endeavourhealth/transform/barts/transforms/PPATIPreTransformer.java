package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.PPATI;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.Condition;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class PPATIPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPATIPreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        //each record is for a separate patient and causes a lot of DB activity, so use a thread pool to parallelise it
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 10000);

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {

                    //no try/catch for record-level errors, as errors in this transform mean we can't continue
                    createPatient((PPATI)parser, fhirResourceFiler, csvHelper, threadPool);
                }
            }

        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            handleErrors(errors);
        }
    }

    private static void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since they'll most-likely be the same
        ThreadPoolError first = errors.get(0);
        Throwable exception = first.getException();
        PPATIPreTransformCallable callable = (PPATIPreTransformCallable)first.getCallable();
        CsvCurrentState parserState = callable.getParserState();
        throw new TransformException(parserState.toString(), exception);
    }

    public static void createPatient(PPATI parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper, ThreadPool threadPool) throws Exception {

        //in-active (i.e. deleted) rows don't have anything else but the ID, so we can't do anything with them
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            return;
        }

        CsvCell personIdCell = parser.getMillenniumPersonId();
        CsvCell mrnCell = parser.getLocalPatientId();

        if (mrnCell.isEmpty()) {
            return;
        }

        PPATIPreTransformCallable callable = new PPATIPreTransformCallable(parser.getCurrentState(), personIdCell, mrnCell, csvHelper, fhirResourceFiler);
        List<ThreadPoolError> errors = threadPool.submit(callable);
        handleErrors(errors);
    }



    private static void moveProblems(String oldPersonId, CsvCell newPersonIdCell, BartsCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        //find the old patient UUID
        UUID oldPatientUuid = IdHelper.getOrCreateEdsResourceId(fhirResourceFiler.getServiceId(), ResourceType.Patient, oldPersonId);

        ResourceDalI dal = DalProvider.factoryResourceDal();
        List<ResourceWrapper> resourceWrappers = dal.getResourcesByPatient(fhirResourceFiler.getServiceId(), oldPatientUuid, ResourceType.Condition.toString());
        for (ResourceWrapper wrapper: resourceWrappers) {
            if (wrapper.isDeleted()) {
                continue;
            }

            String json = wrapper.getResourceData();
            Condition condition = (Condition) FhirSerializationHelper.deserializeResource(json);
            ConditionBuilder builder = new ConditionBuilder(condition);

            //non-problem conditions are sorted in the DIANG transformer
            if (!builder.isProblem()) {
                continue;
            }

            Reference patientReference = csvHelper.createPatientReference(newPersonIdCell);
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
            builder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, builder);
        }
    }

    private static void moveEpisodes(String oldPersonId, CsvCell newPersonIdCell, BartsCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        //find the old patient UUID
        UUID oldPatientUuid = IdHelper.getOrCreateEdsResourceId(fhirResourceFiler.getServiceId(), ResourceType.Patient, oldPersonId);

        ResourceDalI dal = DalProvider.factoryResourceDal();
        List<ResourceWrapper> resourceWrappers = dal.getResourcesByPatient(fhirResourceFiler.getServiceId(), oldPatientUuid, ResourceType.EpisodeOfCare.toString());
        for (ResourceWrapper wrapper: resourceWrappers) {
            if (wrapper.isDeleted()) {
                continue;
            }

            String json = wrapper.getResourceData();
            EpisodeOfCare episodeOfCare = (EpisodeOfCare)FhirSerializationHelper.deserializeResource(json);
            EpisodeOfCareBuilder builder = new EpisodeOfCareBuilder(episodeOfCare);

            Reference patientReference = csvHelper.createPatientReference(newPersonIdCell);
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
            builder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, builder);
        }
    }


    static class PPATIPreTransformCallable implements Callable {

        private CsvCurrentState parserState = null;
        private CsvCell personIdCell = null;
        private CsvCell mrnCell = null;
        private BartsCsvHelper csvHelper = null;
        private FhirResourceFiler fhirResourceFiler = null;

        public PPATIPreTransformCallable(CsvCurrentState parserState,
                                CsvCell personIdCell,
                                CsvCell mrnCell,
                                 BartsCsvHelper csvHelper,
                                 FhirResourceFiler fhirResourceFiler) {

            this.parserState = parserState;
            this.personIdCell = personIdCell;
            this.mrnCell = mrnCell;
            this.csvHelper = csvHelper;
            this.fhirResourceFiler = fhirResourceFiler;
        }

        @Override
        public Object call() throws Exception {

            try {

                //the Data Warehouse files all use PersonID as the unique local identifier for patients, but the
                //ADT feed uses the MRN, so we need to ensure that the Discovery UUID is the same as used by the ADT feed
                String localUniqueId = personIdCell.getString();
                String hl7ReceiverUniqueId = "PIdAssAuth=" + BartsCsvToFhirTransformer.PRIMARY_ORG_HL7_OID + "-PatIdValue=" + mrnCell.getString(); //this must match the HL7 Receiver
                String hl7ReceiverScope = csvHelper.getHl7ReceiverScope();
                csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.Patient, localUniqueId, hl7ReceiverUniqueId, hl7ReceiverScope);

                String currentMrn = mrnCell.getString();
                String personId = personIdCell.getString();

                //the Problem file only contains MRN, so we need to maintain the map of MRN -> PERSON ID, so it can find the patient UUID
                //but we need to handle that there are some rare cases (about 16 in the first half of 2018) where two PPATI
                //records can have the MRN moved from one record to another. For all other files (e.g. ENCNT, CLEVE) we
                //get updates to them, moving them to the new Person ID, but problems must be done manually
                String originalPersonIdForMrn = csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, personId);
                if (Strings.isNullOrEmpty(originalPersonIdForMrn)
                        || !originalPersonIdForMrn.equals(personId)) {

                    if (!Strings.isNullOrEmpty(originalPersonIdForMrn)
                        && !personId.equals(originalPersonIdForMrn)) {

                        moveProblems(originalPersonIdForMrn, personIdCell, csvHelper, fhirResourceFiler);
                        moveEpisodes(originalPersonIdForMrn, personIdCell, csvHelper, fhirResourceFiler);
                    }

                    //and update the mapping
                    csvHelper.saveInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, currentMrn, personId);
                }

                //also store the person ID -> MRN mapping which we use to try to match to resources created by the ADT feed
                String originalMrnForPersonId = csvHelper.getInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personId);
                if (Strings.isNullOrEmpty(originalMrnForPersonId)
                        || !originalMrnForPersonId.equals(currentMrn)) {

                    csvHelper.saveInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personId, currentMrn);
                }

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }

        public CsvCurrentState getParserState() {
            return parserState;
        }
    }
}

