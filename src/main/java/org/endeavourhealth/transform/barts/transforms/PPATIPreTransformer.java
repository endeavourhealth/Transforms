package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.PPATI;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.ParserI;
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

public class PPATIPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPATIPreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch for record-level errors, as errors in this transform mean we can't continue
                createPatient((PPATI)parser, fhirResourceFiler, csvHelper);
            }
        }
    }

    public static void createPatient(PPATI parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

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

        //the Data Warehouse files all use PersonID as the unique local identifier for patients, but the
        //ADT feed uses the MRN, so we need to ensure that the Discovery UUID is the same as used by the ADT feed
        String localUniqueId = personIdCell.getString();
        String hl7ReceiverUniqueId = "PIdAssAuth=" + BartsCsvToFhirTransformer.PRIMARY_ORG_HL7_OID + "-PatIdValue=" + mrnCell.getString(); //this must match the HL7 Receiver
        String hl7ReceiverScope = csvHelper.getHl7ReceiverScope();
        csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.Patient, localUniqueId, hl7ReceiverUniqueId, hl7ReceiverScope);

        //the Problem file only contains MRN, so we need to maintain the map of MRN -> PERSON ID, so it can find the patient UUID
        //but we need to handle that there are some rare cases (about 16 in the first half of 2018) where two PPATI
        //records can have the MRN moved from one record to another. For all other files (e.g. ENCNT, CLEVE) we
        //get updates to them, moving them to the new Person ID, but problems must be done manually
        String originalPersonIdForMrn = csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, personIdCell.getString());
        if (!Strings.isNullOrEmpty(originalPersonIdForMrn)) {
            if (!personIdCell.getString().equals(originalPersonIdForMrn)) {

                moveProblems(originalPersonIdForMrn, personIdCell, csvHelper, fhirResourceFiler);
                moveEpisodes(originalPersonIdForMrn, personIdCell, csvHelper, fhirResourceFiler);

                //and update the mapping
                csvHelper.saveInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, mrnCell.getString(), personIdCell.getString());
            }
        }

        //also store the person ID -> MRN mapping which we use to try to match to resources created by the ADT feed
        csvHelper.saveInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personIdCell.getString(), mrnCell.getString());
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
}

