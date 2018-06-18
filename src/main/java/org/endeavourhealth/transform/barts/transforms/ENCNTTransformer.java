package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.cache.EpisodeOfCareCache;
import org.endeavourhealth.transform.barts.schema.ENCNT;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ENCNTTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ENCNTTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createEncounter((ENCNT)parser, fhirResourceFiler, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createEncounter(ENCNT parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // Check if encounter type should be excluded
        CsvCell encounterTypeCodeCell = parser.getEncounterTypeMillenniumCode();
        if (excludeEncounterType(encounterTypeCodeCell.getString())) {
            return;
        }

        CsvCell encounterIdCell = parser.getMillenniumEncounterIdentifier();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        CsvCell activeCell = parser.getActiveIndicator();

        //this will save a little bit of DB reading when we get to the PROCE and DIAGN transforms
        csvHelper.cacheEncounterIdToPersonId(encounterIdCell, personIdCell);

        EncounterBuilder encounterBuilder = EncounterResourceCache.getEncounterBuilder(encounterIdCell, personIdCell, activeCell, csvHelper);

        //if inactive, we want to delete it
        if (!activeCell.getIntAsBoolean()) {
            EncounterResourceCache.deleteEncounter(encounterBuilder, encounterIdCell, fhirResourceFiler, parser.getCurrentState());
            return;
        }

        CsvCell episodeIdentiferCell = parser.getEpisodeIdentifier();


        CsvCell finIdCell = parser.getMillenniumFinancialNumberIdentifier();
        CsvCell visitIdCell = parser.getMilleniumSourceIdentifierForVisit();
        CsvCell treatmentFunctionCodeCell = parser.getCurrentTreatmentFunctionMillenniumCode();
        CsvCell currentMainSpecialtyMillenniumCodeCell = parser.getCurrentMainSpecialtyMillenniumCode();


        // Retrieve or create EpisodeOfCare
        EpisodeOfCareBuilder episodeOfCareBuilder = EpisodeOfCareCache.getEpisodeOfCareBuilder(episodeIdentiferCell, finIdCell, encounterIdCell, personIdCell, csvHelper);

        encounterBuilder.setEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeOfCareBuilder.getResourceId()), finIdCell);

        // Create new encounter
        if (encounterBuilder != null) {
//TODO - restore this

/*            // Has patient reference changed?
            String currentPatientUuid = ReferenceHelper.getReferenceId(encounterBuilder.getPatient());
            if (currentPatientUuid.compareToIgnoreCase(patientUuid.toString()) != 0) {
                // As of 2018-03-02 we dont appear to get any further ENCNT entries for teh minor encounter in A35 and hence the EoC reference on an encounter cannot change
                // PatientTable reference on EncounterTable resources is handled below
                // PatientTable reference on EpisodeOfCare resources is handled below
                changeOfPatient = true;
                LOG.debug("EncounterTable has changed patient from " + currentPatientUuid + " to " + patientUuid.toString());

                List<Resource> resourceList = csvHelper.retrieveResourceByPatient(UUID.fromString(currentPatientUuid));
                for (Resource resource : resourceList) {
                    if (resource instanceof Condition) {
                        ConditionBuilder builder = new ConditionBuilder((Condition) resource);
                        builder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
                        fhirResourceFiler.savePatientResource(parser.getCurrentState(), builder);
                    } else if (resource instanceof Procedure) {
                        ProcedureBuilder builder = new ProcedureBuilder((Procedure) resource);
                        builder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
                        fhirResourceFiler.savePatientResource(parser.getCurrentState(), builder);
                    } else if (resource instanceof Observation) {
                        ObservationBuilder builder = new ObservationBuilder((Observation) resource);
                        builder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
                        fhirResourceFiler.savePatientResource(parser.getCurrentState(), builder);
                    } else {
                        throw new TransformException("Unsupported resource type " + resource.getClass());
                    }
                }
            }*/
        }

        // Save visit-id to encounter-id link
        if (visitIdCell != null && !visitIdCell.isEmpty()) {
            csvHelper.saveInternalId(InternalIdMap.TYPE_VISIT_ID_TO_ENCOUNTER_ID, visitIdCell.getString(), encounterIdCell.getString());
        }

        //if (changeOfPatient) {
            // Re-establish EpisodeOfCare
        //}

        // Identifiers
        if (!finIdCell.isEmpty()) {
            List<Identifier> identifiers = IdentifierBuilder.findExistingIdentifiersForSystem(encounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID);
            if (identifiers.size() > 0) {
                encounterBuilder.getIdentifiers().remove(identifiers.get(0));
            }
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID);
            identifierBuilder.setUse(Identifier.IdentifierUse.TEMP);
            identifierBuilder.setValue(finIdCell.getString(), finIdCell);
        }

        if (!visitIdCell.isEmpty()) {
            List<Identifier> identifiers = IdentifierBuilder.findExistingIdentifiersForSystem(encounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_VISIT_NO_EPISODE_ID);
            if (identifiers.size() > 0) {
                encounterBuilder.getIdentifiers().remove(identifiers.get(0));
            }
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_VISIT_NO_EPISODE_ID);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setValue(visitIdCell.getString(), visitIdCell);
        }

        if (!encounterIdCell.isEmpty()) {
            List<Identifier> identifiers = IdentifierBuilder.findExistingIdentifiersForSystem(encounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_ENCOUNTER_ID);
            if (identifiers.size() > 0) {
                encounterBuilder.getIdentifiers().remove(identifiers.get(0));
            }
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_ENCOUNTER_ID);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setValue(encounterIdCell.getString(), encounterIdCell);

            String checkDest = csvHelper.getInternalId(InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCell.getString());
            if (checkDest == null) {
                csvHelper.saveInternalId(InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCell.getString(), episodeOfCareBuilder.getResourceId());
            } else {
                if (checkDest.compareToIgnoreCase(episodeOfCareBuilder.getResourceId()) != 0) {
                    TransformWarnings.log(LOG, parser, "EncounterTable {} previously pointed to EoC {} but this has changed to {} in file {}", encounterIdCell.getString(), checkDest, episodeOfCareBuilder.getResourceId(), parser.getFilePath());
                    csvHelper.saveInternalId(InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCell.getString(), episodeOfCareBuilder.getResourceId());
                }
            }
        }

        // class
        //fhirEncounter.setClass_(getEncounterClass(parser.getEncounterTypeMillenniumCode()));
        encounterBuilder.setClass(getEncounterClass(encounterTypeCodeCell.getString(), encounterIdCell, parser), encounterTypeCodeCell);

        // status
        //Date d = null;
        CsvCell status = parser.getEncounterStatusMillenniumCode();
        encounterBuilder.setStatus(getEncounterStatus(status.getString(), encounterIdCell, parser), status);

        //Reason
        CsvCell reasonForVisit = parser.getReasonForVisitText();
        if (!reasonForVisit.isEmpty()) {
            encounterBuilder.addReason(reasonForVisit.getString(), reasonForVisit);
        }

        // EncounterTable type
        BartsCodeableConceptHelper.applyCodeDisplayTxt(encounterTypeCodeCell, CodeValueSet.ENCOUNTER_TYPE, encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Admission_Type, csvHelper);

        // treatment function
        BartsCodeableConceptHelper.applyCodeDisplayTxt(treatmentFunctionCodeCell, CodeValueSet.TREATMENT_FUNCTION, encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Treatment_Function, csvHelper);

        // EpisodeOfCare
        encounterBuilder.setEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeOfCareBuilder.getResourceId()), episodeIdentiferCell);

        // Referrer
        CsvCell referrerPersonnelIdentifier = parser.getReferrerMillenniumPersonnelIdentifier();
        if (!BartsCsvHelper.isEmptyOrIsZero(referrerPersonnelIdentifier)) {

            Reference practitionerReference = csvHelper.createPractitionerReference(referrerPersonnelIdentifier);
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }

            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.REFERRER, true, referrerPersonnelIdentifier);
        }

        // responsible person
        CsvCell responsibleHCPCell = parser.getResponsibleHealthCareprovidingPersonnelIdentifier();
        if (!BartsCsvHelper.isEmptyOrIsZero(responsibleHCPCell)) {

            Reference practitionerReference = csvHelper.createPractitionerReference(responsibleHCPCell);
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }

            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER, true, responsibleHCPCell);
        }

        // registering person
        CsvCell registeringPersonnelIdentifierCell = parser.getRegisteringMillenniumPersonnelIdentifier();
        if (!BartsCsvHelper.isEmptyOrIsZero(registeringPersonnelIdentifierCell)) {

            Reference practitionerReference = csvHelper.createPractitionerReference(registeringPersonnelIdentifierCell);
            if (encounterBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }

            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.PARTICIPANT, true, registeringPersonnelIdentifierCell);
        }

        // Location
        // Field maintained from OPATT, AEATT, IPEPI and IPWDS

        if (currentMainSpecialtyMillenniumCodeCell != null && !currentMainSpecialtyMillenniumCodeCell.isEmpty()) {
//TODO - restore this
            /*ResourceId specialtyResourceid = getOrCreateSpecialtyResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, currentMainSpecialtyMillenniumCodeCell.getString());
            if (specialtyResourceid != null) {
                encounterBuilder.setServiceProvider(ReferenceHelper.createReference(ResourceType.Organization, specialtyResourceid.getResourceId().toString()),currentMainSpecialtyMillenniumCodeCell);
            }*/
        }

        // Maintain EpisodeOfCare
        // Field maintained from OPATT, AEATT, IPEPI and IPWDS


    }

    private static boolean excludeEncounterType(String millenniumCode) throws Exception {
        if (millenniumCode.compareTo("309313") == 0) { return true; } // Inpatient Pre-Admission
        else if (millenniumCode.compareTo("3767801") == 0) { return true; } // Inpatient Waiting List
        else if (millenniumCode.compareTo("3767802") == 0) { return true; } // Day Case Waiting List
        else if (millenniumCode.compareTo("3767803") == 0) { return true; } // Outpatient Referral
        else if (millenniumCode.compareTo("3767806") == 0) { return true; } // Outpatient Pre-registration
        else if (millenniumCode.compareTo("3768747") == 0) { return true; } // Outpatient Services
        else {
            return false;
        }
    }

    /*
    *
     */
    private static Encounter.EncounterClass getEncounterClass(String millenniumCode, CsvCell encounterIdCell,  ParserI parser) throws Exception {
        if (millenniumCode.compareTo("309308") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("309309") == 0) { return Encounter.EncounterClass.OUTPATIENT; }
        else if (millenniumCode.compareTo("309313") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3767801") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3767802") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3767803") == 0) { return Encounter.EncounterClass.OUTPATIENT; }
        else if (millenniumCode.compareTo("3767804") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3767806") == 0) { return Encounter.EncounterClass.OUTPATIENT; }
        else if (millenniumCode.compareTo("3767807") == 0) { return Encounter.EncounterClass.EMERGENCY; }
        else if (millenniumCode.compareTo("3767808") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3767809") == 0) { return Encounter.EncounterClass.OUTPATIENT; }
        else if (millenniumCode.compareTo("3767810") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3767811") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3767812") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.compareTo("3768747") == 0) { return Encounter.EncounterClass.OUTPATIENT; }
        else if (millenniumCode.compareTo("3768748") == 0) { return Encounter.EncounterClass.INPATIENT; }
        else {
            TransformWarnings.log(LOG, parser, "Millennium encouter-type {} not found for Personnel-id {} in ENCNT record {} in file {}", millenniumCode, encounterIdCell.getString(), parser.getFilePath());
            return Encounter.EncounterClass.OTHER;
        }
    }

    /*
    *
     */
    private static Encounter.EncounterState getEncounterStatus(String millenniumCode, CsvCell encounterIdCell,  ParserI parser) throws Exception {
        if (millenniumCode.compareTo("666807") == 0) { return Encounter.EncounterState.CANCELLED; }
        else if (millenniumCode.compareTo("666808") == 0) { return Encounter.EncounterState.PLANNED; }
        else if (millenniumCode.compareTo("666809") == 0) { return Encounter.EncounterState.PLANNED; }
        else if (millenniumCode.compareTo("854") == 0) { return Encounter.EncounterState.INPROGRESS; }
        else if (millenniumCode.compareTo("855") == 0) { return Encounter.EncounterState.CANCELLED; }
        else if (millenniumCode.compareTo("856") == 0) { return Encounter.EncounterState.FINISHED; }
        else if (millenniumCode.compareTo("857") == 0) { return Encounter.EncounterState.INPROGRESS; }
        else if (millenniumCode.compareTo("858") == 0) { return Encounter.EncounterState.ARRIVED; }
        else if (millenniumCode.compareTo("859") == 0) { return Encounter.EncounterState.PLANNED; }
        else if (millenniumCode.compareTo("860") == 0) { return Encounter.EncounterState.INPROGRESS; }
        else {
            TransformWarnings.log(LOG, parser, "Millennium status-code {} not found for Personnel-id {} in ENCNT record {} in file {}. Status set to in-progress", millenniumCode, encounterIdCell.getString(), parser.getFilePath());
            return Encounter.EncounterState.INPROGRESS;
        }
    }

}
