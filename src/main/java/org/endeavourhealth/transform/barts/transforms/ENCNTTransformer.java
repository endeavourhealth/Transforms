package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.schema.ENCNT;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class ENCNTTransformer extends BartsBasisTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ENCNTTransformer.class);

    /*
     *
     */
    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    String valStr = validateEntry((ENCNT) parser);
                    if (valStr == null) {
                        createEncounter((ENCNT) parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                    } else {
                        TransformWarnings.log(LOG, parser, "Validation error: {}", valStr);
                    }
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    /*
     *
     */
    public static String validateEntry(ENCNT parser) {
        return null;
    }


    /*
     *
     */
    public static void createEncounter(ENCNT parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        boolean changeOfPatient = false;
        EpisodeOfCareBuilder episodeOfCareBuilder = null;
        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell encounterIdCell = parser.getMillenniumEncounterIdentifier();
        CsvCell episodeIdentiferCell = parser.getEpisodeIdentifier();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        CsvCell encounterTypeCodeCell = parser.getEncounterTypeMillenniumCode();
        CsvCell finIdCell = parser.getMillenniumFinancialNumberIdentifier();
        CsvCell visitIdCell = parser.getMilleniumSourceIdentifierForVisit();
        CsvCell treatmentFunctionCodeCell = parser.getCurrentTreatmentFunctionMillenniumCode();
        CsvCell currentMainSpecialtyMillenniumCodeCell = parser.getCurrentMainSpecialtyMillenniumCode();

        EncounterBuilder encounterBuilder = EncounterResourceCache.getEncounterBuilder(csvHelper, encounterIdCell.getString());
        if (encounterBuilder == null && !activeCell.getIntAsBoolean()) {
            // skip - encounter missing but set to delete so do nothing
            return;
        }

        // Patient
        UUID patientUuid = csvHelper.findPatientIdFromPersonId(personIdCell);
        if (patientUuid == null) {
            TransformWarnings.log(LOG, parser, "Skipping encounter {} because no Person->MRN mapping {} could be found in file {}", encounterIdCell.getString(), personIdCell.getString(), parser.getFilePath());
            return;
        }

        // Delete existing encounter ?
        if (encounterBuilder != null && !activeCell.getIntAsBoolean()) {
            encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
            //LOG.debug("Delete Encounter (PatId=" + personIdCell.getString() + "):" + FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
            EncounterResourceCache.deleteEncounterBuilder(encounterBuilder);
            return;
        }

        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Retrieve or create EpisodeOfCare
        episodeOfCareBuilder = readOrCreateEpisodeOfCareBuilder(episodeIdentiferCell, finIdCell, encounterIdCell, personIdCell, patientUuid, csvHelper, parser);
        LOG.debug("episodeOfCareBuilder:" + FhirSerializationHelper.serializeResource(episodeOfCareBuilder.getResource()));
        if (encounterBuilder != null && episodeOfCareBuilder.getResourceId().compareToIgnoreCase(ReferenceHelper.getReferenceId(encounterBuilder.getEpisodeOfCare().get(0))) != 0) {
            LOG.debug("episodeOfCare reference has changed from " + encounterBuilder.getEpisodeOfCare().get(0).getReference() + " to " + episodeOfCareBuilder.getResourceId());
        }

        // Create new encounter
        if (encounterBuilder == null) {
            encounterBuilder = EncounterResourceCache.createEncounterBuilder(encounterIdCell, finIdCell);

            encounterBuilder.addEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeOfCareBuilder.getResourceId()), finIdCell);
        } else {

            // Has patient reference changed?
            String currentPatientUuid = ReferenceHelper.getReferenceId(encounterBuilder.getPatient());
            if (currentPatientUuid.compareToIgnoreCase(patientUuid.toString()) != 0) {
                // As of 2018-03-02 we dont appear to get any further ENCNT entries for teh minor encounter in A35 and hence the EoC reference on an encounter cannot change
                // Patient reference on Encounter resources is handled below
                // Patient reference on EpisodeOfCare resources is handled below
                changeOfPatient = true;
                LOG.debug("Encounter has changed patient from " + currentPatientUuid + " to " + patientUuid.toString());

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
                    }
                }
            }
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
                    TransformWarnings.log(LOG, parser, "Encounter {} previously pointed to EoC {} but this has changed to {} in file {}", encounterIdCell.getString(), checkDest, episodeOfCareBuilder.getResourceId(), parser.getFilePath());
                    csvHelper.saveInternalId(InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCell.getString(), episodeOfCareBuilder.getResourceId());
                }
            }
        }

        // Patient
        encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);

        episodeOfCareBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);

        // Organisation
        episodeOfCareBuilder.setManagingOrganisation((ReferenceHelper.createReference(ResourceType.Organization, organisationResourceId.getResourceId().toString())));


        // class
        //fhirEncounter.setClass_(getEncounterClass(parser.getEncounterTypeMillenniumCode()));
        encounterBuilder.setClass(getEncounterClass(encounterTypeCodeCell.getString(), encounterIdCell, parser), encounterTypeCodeCell);

        // status
        //Date d = null;
        CsvCell status = parser.getEncounterStatusMillenniumCode();
        encounterBuilder.setStatus(getEncounterStatus(status.getString(), encounterIdCell, parser), status);

        //Reason
        CsvCell reasonForVisit = parser.getReasonForVisitText();
        CodeableConcept reasonForVisitText = CodeableConceptHelper.createCodeableConcept(reasonForVisit.getString());
        encounterBuilder.addReason(reasonForVisitText, true, reasonForVisit);

        // Encounter type
        BartsCodeableConceptHelper.applyCodeDisplayTxt(encounterTypeCodeCell, CernerCodeValueRef.ENCOUNTER_TYPE, encounterBuilder, EncounterBuilder.TAG_ENCOUNTER_ADMISSION_TYPE, csvHelper);

        /*if (!encounterTypeCodeCell.isEmpty() && encounterTypeCodeCell.getLong() > 0) {
            CernerCodeValueRef ret = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                                                            CernerCodeValueRef.PERSONNEL_SPECIALITY,
                                                            encounterTypeCodeCell.getLong(),
                                                            fhirResourceFiler.getServiceId());

            String encounterDispTxt = ret.getCodeDispTxt();
            CodeableConcept fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_SPECIALTY, encounterDispTxt, encounterTypeCodeCell.getString());
            encounterBuilder.addExtension(FhirExtensionUri.ENCOUNTER_SPECIALTY, fhirCodeableConcept, encounterTypeCodeCell);
        }*/

        // treatment function
        BartsCodeableConceptHelper.applyCodeDisplayTxt(treatmentFunctionCodeCell, CernerCodeValueRef.TREATMENT_FUNCTION, encounterBuilder, EncounterBuilder.TAG_TREATMENT_FUNCTION, csvHelper);

        /*if (!treatmentFunctionCodeCell.isEmpty() && treatmentFunctionCodeCell.getLong() > 0) {
            CernerCodeValueRef ret = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                                                            CernerCodeValueRef.TREATMENT_FUNCTION,
                                                            treatmentFunctionCodeCell.getLong(),
                                                            fhirResourceFiler.getServiceId());

            String treatFuncDispTxt = ret.getCodeDispTxt();
            CodeableConcept fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_TREATMENT_FUNCTION, treatFuncDispTxt, treatmentFunctionCodeCell.getString());
            encounterBuilder.addExtension(FhirExtensionUri.ENCOUNTER_TREATMENT_FUNCTION, fhirCodeableConcept);
        }*/

        // EpisodeOfCare
        encounterBuilder.addEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeOfCareBuilder.getResourceId()), episodeIdentiferCell);

        // Referrer
        CsvCell referrerPersonnelIdentifier = parser.getReferrerMillenniumPersonnelIdentifier();
        if (!referrerPersonnelIdentifier.isEmpty()) {
            ResourceId referrerPersonResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, referrerPersonnelIdentifier);
            if (referrerPersonResourceId != null) {
                encounterBuilder.addParticipant(csvHelper.createPractitionerReference(referrerPersonResourceId.getResourceId().toString()), EncounterParticipantType.REFERRER, true, referrerPersonnelIdentifier);
            } else {
                TransformWarnings.log(LOG, parser, "Practitioner Resource not found for Referrer-id {} in ENCNT record {} in file {}", parser.getReferrerMillenniumPersonnelIdentifier(), parser.getMillenniumEncounterIdentifier(), parser.getFilePath());
            }
        }

        // responsible person
        CsvCell responsibleHCPCell = parser.getResponsibleHealthCareprovidingPersonnelIdentifier();
        if (!responsibleHCPCell.isEmpty()) {
            ResourceId respPersonResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, responsibleHCPCell);
            if (respPersonResourceId != null) {
                encounterBuilder.addParticipant(csvHelper.createPractitionerReference(respPersonResourceId.getResourceId().toString()), EncounterParticipantType.PRIMARY_PERFORMER, true, responsibleHCPCell);
            } else {
                TransformWarnings.log(LOG, parser, "Practitioner Resource not found for Personnel-id {} in ENCNT record {} in file {}", parser.getResponsibleHealthCareprovidingPersonnelIdentifier(), parser.getMillenniumEncounterIdentifier(), parser.getFilePath());
            }
        }

        // registering person
        CsvCell registeringPersonnelIdentifierCell = parser.getRegisteringMillenniumPersonnelIdentifier();
        if (!registeringPersonnelIdentifierCell.isEmpty()) {
            ResourceId regPersonResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, registeringPersonnelIdentifierCell);
            if (regPersonResourceId != null) {
                encounterBuilder.addParticipant(csvHelper.createPractitionerReference(regPersonResourceId.getResourceId().toString()), EncounterParticipantType.PARTICIPANT, true, registeringPersonnelIdentifierCell);
            } else {
                TransformWarnings.log(LOG, parser, "Practitioner Resource not found for Personnel-id {} in ENCNT record {} in file {}", parser.getRegisteringMillenniumPersonnelIdentifier(), parser.getMillenniumEncounterIdentifier(), parser.getFilePath());
            }
        }

        // Location
        // Field maintained from OPATT, AEATT, IPEPI and IPWDS

        if (currentMainSpecialtyMillenniumCodeCell != null && !currentMainSpecialtyMillenniumCodeCell.isEmpty()) {
            ResourceId specialtyResourceid = getOrCreateSpecialtyResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, currentMainSpecialtyMillenniumCodeCell.getString());
            if (specialtyResourceid != null) {
                encounterBuilder.setServiceProvider(ReferenceHelper.createReference(ResourceType.Organization, specialtyResourceid.getResourceId().toString()),currentMainSpecialtyMillenniumCodeCell);
            }
        }

        //cache our encounter details so subsequent transforms can use them
        csvHelper.cacheEncounterIds(encounterIdCell, (Encounter)encounterBuilder.getResource());

        // Maintain EpisodeOfCare
        // Field maintained from OPATT, AEATT, IPEPI and IPWDS

        if (LOG.isDebugEnabled()) {
            LOG.debug("episodeOfCare Complete:" + FhirSerializationHelper.serializeResource(episodeOfCareBuilder.getResource()));
            LOG.debug("encounter complete:" + FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
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
