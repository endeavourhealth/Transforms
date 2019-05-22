package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
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

import java.util.List;
import java.util.UUID;

public class ENCNTPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ENCNTTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {
                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }
                    //no try/catch here, because any error means it's not safe to continue
                    processRecord((ENCNT)parser, fhirResourceFiler, csvHelper);
                }
            }
        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }

    }

    /**
     * this pre-transformer tries to match the Data Warehouse Encounters to the HL7 Receiver Encounters
     * which needs the MRN and VISIT ID
     */
    public static void processRecord(ENCNT parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

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

        PreTransformCallable callable
                = new PreTransformCallable(parser.getCurrentState(),
                                           personIdCell,
                                           encounterIdCell,
                                           responsiblePersonnelIdCell,
                                           episodeIdCell,
                                           finCell,
                                           visitIdCell,
                                           csvHelper,
                                           fhirResourceFiler);

        csvHelper.submitToThreadPool(callable);
    }


    static class PreTransformCallable extends AbstractCsvCallable {

        private CsvCell personIdCell;
        private CsvCell encounterIdCell;
        private CsvCell responsiblePersonnelIdCell;
        private CsvCell episodeIdCell;
        private CsvCell finCell;
        private CsvCell visitIdCell;
        private BartsCsvHelper csvHelper;
        private FhirResourceFiler fhirResourceFiler;

        public PreTransformCallable(CsvCurrentState parserState,
                                    CsvCell personIdCell,
                                    CsvCell encounterIdCell,
                                    CsvCell responsiblePersonnelIdCell,
                                    CsvCell episodeIdCell,
                                    CsvCell finCell,
                                    CsvCell visitIdCell,
                                    BartsCsvHelper csvHelper,
                                    FhirResourceFiler fhirResourceFiler) {
            super(parserState);
            this.personIdCell = personIdCell;
            this.encounterIdCell = encounterIdCell;
            this.responsiblePersonnelIdCell = responsiblePersonnelIdCell;
            this.episodeIdCell = episodeIdCell;
            this.finCell = finCell;
            this.visitIdCell = visitIdCell;
            this.csvHelper = csvHelper;
            this.fhirResourceFiler = fhirResourceFiler;
        }

        @Override
        public Object call() throws Exception {

            try {

                //see if the person ID has changed for this encounter - if it has, then
                //we'll need to move all dependent objects to the new patient
                String existingPersonId = csvHelper.findPersonIdFromEncounterId(encounterIdCell);
                if (!Strings.isNullOrEmpty(existingPersonId)
                        && !existingPersonId.equals(personIdCell.getString())) {

                    moveEncounterDependentData(existingPersonId);
                }

                //update the internal ID map with our encounter to person mapping
                csvHelper.saveEncounterIdToPersonId(encounterIdCell, personIdCell);

                //update the internal ID map with our encounter to responsible personnel mapping - DAB-121 enhancement
                csvHelper.saveEncounterIdToResponsiblePersonellId(encounterIdCell, responsiblePersonnelIdCell);

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
                csvHelper.getEpisodeOfCareCache().setUpEpisodeOfCareBuilderMappings(encounterIdCell, personIdCell, episodeIdCell, finCell, visitIdCell);


            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }

        /**
         * if an ENCNT record is moved from on person record to another, we need to manually move any dependent
         * resources (e.g. procedures)
         */
        private void moveEncounterDependentData(String existingPersonId) throws Exception {
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

                } else {
                    //most resources haven't had the right stuff above implemented, so
                    throw new Exception("Unhandled resource type " + resource.getResourceType() + " when moving encounter to another patient " + existingPersonId + " -> " + personIdCell.getString());
                }
            }
        }
    }
}
