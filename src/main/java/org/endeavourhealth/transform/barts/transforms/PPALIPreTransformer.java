package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PPALI;
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

public class PPALIPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPALIPreTransformer.class);

    public static final String PPALI_ID_TO_PERSON_ID = "PPALI_ID_TO_PERSON_ID";

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        //we need to write a lot of stuff to the DB and each record is independent, so use a thread pool to parallelise
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 10000);

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {

                    //no try/catch as failures here meant we should abort
                    processRecord((PPALI)parser, fhirResourceFiler, csvHelper, threadPool);
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
        PPALIPreTransformCallable callable = (PPALIPreTransformCallable)first.getCallable();
        CsvCurrentState parserState = callable.getParserState();
        throw new TransformException(parserState.toString(), exception);
    }


    public static void processRecord(PPALI parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper, ThreadPool threadPool) throws Exception {

        //if non-active (i.e. deleted) we should REMOVE the identifier, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the identifier
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            return;
        }

        //we need to store a mapping of alias ID to person ID
        CsvCell aliasIdCell = parser.getMillenniumPersonAliasId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        //if this record represents an active MRN, then we need to save some special mappings too
        CsvCell activeMrnCell = null;

        //work out if this is an active MRN record
        CsvCell endDateCell = parser.getEndEffectiveDate();
        if (BartsCsvHelper.isEmptyOrIsEndOfTime(endDateCell)) {

            CsvCell aliasTypeCodeCell = parser.getAliasTypeCode();
            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookupCodeRef(CodeValueSet.ALIAS_TYPE, aliasTypeCodeCell);
            String aliasDesc = cernerCodeValueRef.getCodeMeaningTxt();
            if (aliasDesc.equalsIgnoreCase("MRN")) {
                activeMrnCell = parser.getAlias();
            }
        }

        PPALIPreTransformCallable callable = new PPALIPreTransformCallable(parser.getCurrentState(), aliasIdCell, personIdCell, activeMrnCell, csvHelper, fhirResourceFiler);
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


    static class PPALIPreTransformCallable implements Callable {

        private CsvCurrentState parserState = null;
        private CsvCell aliasIdCell = null;
        private CsvCell personIdCell = null;
        private CsvCell activeMrnCell = null;
        private BartsCsvHelper csvHelper = null;
        private FhirResourceFiler fhirResourceFiler = null;

        public PPALIPreTransformCallable(CsvCurrentState parserState,
                                         CsvCell aliasIdCell,
                                         CsvCell personIdCell,
                                         CsvCell activeMrnCell,
                                         BartsCsvHelper csvHelper,
                                         FhirResourceFiler fhirResourceFiler) {

            this.parserState = parserState;
            this.aliasIdCell = aliasIdCell;
            this.personIdCell = personIdCell;
            this.activeMrnCell = activeMrnCell;
            this.csvHelper = csvHelper;
            this.fhirResourceFiler = fhirResourceFiler;
        }

        @Override
        public Object call() throws Exception {

            try {

                //we need to store the PPADD ID -> PERSON ID mapping so that if the address is ever deleted,
                //we can find the person it belonged to, since the deleted records only give us the ID
                csvHelper.saveInternalId(PPALI_ID_TO_PERSON_ID, aliasIdCell.getString(), personIdCell.getString());

                if (activeMrnCell != null) {

                    //the Data Warehouse files all use PersonID as the unique local identifier for patients, but the
                    //ADT feed uses the MRN, so we need to ensure that the Discovery UUID is the same as used by the ADT feed
                    String localUniqueId = personIdCell.getString();
                    String hl7ReceiverUniqueId = "PIdAssAuth=" + BartsCsvToFhirTransformer.PRIMARY_ORG_HL7_OID + "-PatIdValue=" + activeMrnCell.getString(); //this must match the HL7 Receiver
                    String hl7ReceiverScope = csvHelper.getHl7ReceiverScope();
                    csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.Patient, localUniqueId, hl7ReceiverUniqueId, hl7ReceiverScope);

                    String currentMrn = activeMrnCell.getString();
                    String personId = personIdCell.getString();

                    //the Problem file only contains MRN, so we need to maintain the map of MRN -> PERSON ID, so it can find the patient UUID
                    //but we need to handle that there are some rare cases (about 16 in the first half of 2018) where two PPATI
                    //records can have the MRN moved from one record to another. For all other files (e.g. ENCNT, CLEVE) we
                    //get updates to them, moving them to the new Person ID, but problems must be done manually
                    String originalPersonIdForMrn = csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, personId);
                    if (Strings.isNullOrEmpty(originalPersonIdForMrn)
                            || !originalPersonIdForMrn.equals(personId)) {

                        //there are some PPATI records where they are clearly intended to be the same patient (due to same DoB)
                        //but one is a "proper" record and the other

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
