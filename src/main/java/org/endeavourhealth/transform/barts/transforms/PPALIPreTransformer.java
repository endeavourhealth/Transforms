package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
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

public class PPALIPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPALIPreTransformer.class);

    public static final String PPALI_ID_TO_PERSON_ID = "PPALI_ID_TO_PERSON_ID";

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {

                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }

                    //no try/catch as failures here meant we should abort
                    processRecord((PPALI)parser, fhirResourceFiler, csvHelper);
                }
            }
        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    public static void processRecord(PPALI parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //if non-active (i.e. deleted) we should REMOVE the identifier, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the identifier
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            return;
        }

        //we need to store a mapping of alias ID to person ID
        CsvCell aliasIdCell = parser.getMillenniumPersonAliasId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        if (personIdCell.isEmpty() || aliasIdCell.isEmpty()) {
            return;
        }

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
        csvHelper.submitToThreadPool(callable);
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


    static class PPALIPreTransformCallable extends AbstractCsvCallable {

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

            super(parserState);
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
                    UUID patientUuid = csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.Patient, localUniqueId, hl7ReceiverUniqueId, hl7ReceiverScope, false);

                    String currentMrn = activeMrnCell.getString();
                    String personId = personIdCell.getString();

                    String originalPersonIdForMrn = csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, currentMrn);
                    if (Strings.isNullOrEmpty(originalPersonIdForMrn)
                            || !originalPersonIdForMrn.equals(personId)) {

                        //update the mapping
                        csvHelper.saveInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, currentMrn, personId);

                        //and if we've detected that our MRN has moved from one patient to another, we need to update a few things
                        if (!Strings.isNullOrEmpty(originalPersonIdForMrn)
                                && !personId.equals(originalPersonIdForMrn)) {

                            //update the HL7 Receiver DB to give it the new patient UUID for the MRN so future ADT
                            //messages for the MRN go against the right patient
                            csvHelper.updateHl7ReceiverWithNewUuid(ResourceType.Patient, hl7ReceiverUniqueId, hl7ReceiverScope, patientUuid);

                            //the Problem file only contains MRN, so we need to maintain the map of MRN -> PERSON ID, so it can find the patient UUID
                            //but we need to handle that there are some rare cases (about 16 in the first half of 2018) where two PPATI
                            //records can have the MRN moved from one record to another. For all other files (e.g. ENCNT, CLEVE) we
                            //get updates to them, moving them to the new Person ID, but problems must be done manually
                            moveProblems(originalPersonIdForMrn, personIdCell, csvHelper, fhirResourceFiler);
                            moveEpisodes(originalPersonIdForMrn, personIdCell, csvHelper, fhirResourceFiler);
                        }
                    }

                    //also store the person ID -> MRN mapping which we use to try to match to resources created by the ADT feed
                    String originalMrnForPersonId = csvHelper.getInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personId);
                    if (Strings.isNullOrEmpty(originalMrnForPersonId)
                            || !originalMrnForPersonId.equals(currentMrn)) {

                        csvHelper.saveInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personId, currentMrn);
                    }
                }

                //pre-cache the patient resource
                csvHelper.getPatientCache().preCachePatientBuilder(personIdCell);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}
