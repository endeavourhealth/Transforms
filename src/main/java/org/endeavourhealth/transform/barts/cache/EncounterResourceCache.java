package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EncounterResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(EncounterResourceCache.class);

    private static Map<Long, EncounterBuilder> encounterBuildersByEncounterId = new HashMap<>();
    private static Set<Long> encounterIdsJustDeleted = new HashSet<>();
    private static Map<Long, UUID> encountersWithChangedPatientUuids = new HashMap<>();

    /**
     * the ENCNT transformer deletes Encounters, and records that this has been done here,
     * so that the later transforms can check, since the deleted Encounter may not have reached the DB yet
     */
    public static void deleteEncounter(EncounterBuilder encounterBuilder, CsvCell encounterIdCell, FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState) throws Exception {

        //null may end up passed in, so just ignore
        if (encounterBuilder == null) {
            return;
        }

        //record that we know it's deleted
        Long encounterId = encounterIdCell.getLong();
        encounterIdsJustDeleted.add(encounterId);

        //remove from the cache
        encounterBuildersByEncounterId.remove(encounterId);

        boolean mapIds = !encounterBuilder.isIdMapped();
        fhirResourceFiler.deletePatientResource(parserState, mapIds, encounterBuilder);
    }




    public static EncounterBuilder getEncounterBuilder(CsvCell encounterIdCell, CsvCell personIdCell, CsvCell activeIndicatorCell, BartsCsvHelper csvHelper) throws Exception {

        Long encounterId = encounterIdCell.getLong();

        //if we know we've deleted it, return null
        if (encounterIdsJustDeleted.contains(encounterId)) {
            return null;
        }

        EncounterBuilder encounterBuilder = encounterBuildersByEncounterId.get(encounterId);
        if (encounterBuilder == null) {

            Encounter encounter = (Encounter)csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, encounterId.toString());
            if (encounter != null) {
                encounterBuilder = new EncounterBuilder(encounter);

                //always set the person ID fresh, in case the record has been moved to another patient, remembering to forward map to a UUID
                //but track the old patient UUID so we can use it to update dependent resources
                if (personIdCell != null) {
                    UUID oldPatientUuid = UUID.fromString(encounter.getId());
                    UUID currentPatientUuid = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.Patient, personIdCell.getString());
                    if (!oldPatientUuid.equals(currentPatientUuid)) {

                        encountersWithChangedPatientUuids.put(encounterId, oldPatientUuid);

                        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, currentPatientUuid.toString());
                        encounterBuilder.setPatient(patientReference, personIdCell);
                    }
                }

            } else {

                //if our CSV record is non-active, it means it's deleted, so don't create a builder
                if (!activeIndicatorCell.getIntAsBoolean()) {
                    encounterIdsJustDeleted.add(encounterId);
                    return null;
                }

                encounterBuilder = new EncounterBuilder();
                encounterBuilder.setId(encounterIdCell.getString(), encounterIdCell);

                //set the patient reference
                if (personIdCell != null) {
                    Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personIdCell.getString());
                    encounterBuilder.setPatient(patientReference, personIdCell);
                } else {
                    //if we've not been given a person ID we can't create the EncounterBuilder
                    return null;
                }
            }

            encounterBuildersByEncounterId.put(encounterId, encounterBuilder);
        }

        return encounterBuilder;
    }




    /*public static EncounterBuilder createEncounterBuilder(CsvCell encounterIdCell, CsvCell finIdCell) throws Exception {

        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterIdCell.getString());

        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterIdCell.getString());
        }

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(encounterResourceId.getResourceId().toString(), encounterIdCell);

        IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_ENCOUNTER_ID);
        identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
        identifierBuilder.setValue(encounterIdCell.getString(), encounterIdCell);

        if (finIdCell != null && !finIdCell.isEmpty()) {
            identifierBuilder = new IdentifierBuilder(encounterBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID);
            identifierBuilder.setValue(finIdCell.getString(), finIdCell);
        }

        encounterBuildersByUuid.put(encounterResourceId.getResourceId(), encounterBuilder);

        return encounterBuilder;
    }*/

    public static void fileEncounterResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.trace("Saving " + encounterBuildersByEncounterId.size() + " encounters to the DB");
        for (Long encounterId: encounterBuildersByEncounterId.keySet()) {
            EncounterBuilder encounterBuilder = encounterBuildersByEncounterId.get(encounterId);

            boolean mapIds = !encounterBuilder.isIdMapped();
            fhirResourceFiler.savePatientResource(null, mapIds, encounterBuilder);
        }
        LOG.trace("Finishing saving " + encounterBuildersByEncounterId.size() + " encounters to the DB");

        //clear down as everything has been saved
        encounterBuildersByEncounterId.clear();
        encountersWithChangedPatientUuids.clear();
        encounterIdsJustDeleted.clear();
    }


    public static UUID getOriginalPatientUuid(CsvCell encounterIdCell) {
        Long encounterId = encounterIdCell.getLong();
        return encountersWithChangedPatientUuids.get(encounterId);
    }
}
