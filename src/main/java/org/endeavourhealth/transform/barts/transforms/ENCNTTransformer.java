package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.ENCNT;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ENCNTTransformer extends BartsBasisTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ENCNTTransformer.class);
    private static InternalIdDalI internalIdDAL = null;

    /*
     *
     */
    public static void transform(String version,
                                 ENCNT parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry(parser);
                if (valStr == null) {
                    createEncounter(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
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

        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell encounterIdCell = parser.getMillenniumEncounterIdentifier();
        CsvCell episodeIdentiferCell = parser.getEpisodeIdentifier();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        CsvCell encounterTypeCodeCell = parser.getEncounterTypeMillenniumCode();
        CsvCell finIdCell = parser.getMillenniumFinancialNumberIdentifier();
        CsvCell visitIdCell = parser.getMilleniumSourceIdentifierForVisit();
        CsvCell treatmentFunctionCodeCell = parser.getCurrentTreatmentFunctionMillenniumCode();

        // Encounter resource id
        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterIdCell.getString());
        if (encounterResourceId == null
                && !activeCell.getIntAsBoolean()) {
            // skip - encounter missing but set to delete so do nothing
            return;
        }

        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterIdCell.getString());
        }

        if (internalIdDAL == null) {
            internalIdDAL = DalProvider.factoryInternalIdDal();
        }

        // Save visit-id to encounter-id link
        //TODO - Peter, can you check this? The mapping type says Encounter->Visit, but the parameters are the other way around
        internalIdDAL.upsertRecord(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_ENCOUNTER_ID_TO_VISIT_ID, visitIdCell.getString(), encounterIdCell.getString());

        // Episode resource id
        ResourceId episodeResourceId = getEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCell.getString());

        // Organisation - only used if placeholder patient resource is created
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Patient
        UUID patientUuid = csvHelper.findPatientIdFromPersonId(personIdCell);
        if (patientUuid == null) {
            LOG.warn("Skipping encounter " + encounterIdCell.getString() + " because no Person->MRN mapping could be found");
            return;
        }

        //Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT , "clinical coding")};

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(encounterResourceId.getResourceId().toString(), encounterIdCell);

        if (!activeCell.getIntAsBoolean()) {
            encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);

            LOG.debug("Delete Encounter (PatId=" + personIdCell.getString() + "):" + FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
            return;
        }


        if (episodeResourceId == null) {
            episodeResourceId = createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCell.getString());

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder();

            //TODO - can we set more fields on the episode of care? Do we know its start date or end date?

            Reference organisationReference = ReferenceHelper.createReference(ResourceType.Organization, organisationResourceId.getResourceId().toString());
            episodeOfCareBuilder.setManagingOrganisation(organisationReference);

            Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString());
            episodeOfCareBuilder.setPatient(patientReference);

            savePatientResource(fhirResourceFiler, parser.getCurrentState(), episodeOfCareBuilder);
        }

        //fhirEncounter.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_ENCOUNTER));
        // Handled in builder now

        // Identifiers
        if (!finIdCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID);
            identifierBuilder.setValue(finIdCell.getString(), finIdCell);
        }

        if (!visitIdCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_VISIT_NO_EPISODE_ID);
            identifierBuilder.setValue(visitIdCell.getString(), visitIdCell);
        }

        if (!encounterIdCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_ENCOUNTER_ID);
            identifierBuilder.setValue(encounterIdCell.getString(), encounterIdCell);
        }

        // Patient
        encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);

        // class
        //fhirEncounter.setClass_(getEncounterClass(parser.getEncounterTypeMillenniumCode()));
        encounterBuilder.setClass(getEncounterClass(encounterTypeCodeCell.getString()), encounterTypeCodeCell);

        // status
        CsvCell status = parser.getEncounterStatusMillenniumCode();
        encounterBuilder.setStatus(getEncounterStatus(status.getString()), status);

        //Reason
        CsvCell reasonForVisit = parser.getReasonForVisitText();
        CodeableConcept reasonForVisitText = CodeableConceptHelper.createCodeableConcept(reasonForVisit.getString());
        encounterBuilder.addReason(reasonForVisitText, reasonForVisit);

        // specialty
        BartsCodeableConceptHelper.applyCodeDisplayTxt(encounterTypeCodeCell, CernerCodeValueRef.PERSONNEL_SPECIALITY, encounterBuilder, EncounterBuilder.TAG_SPECIALTY, fhirResourceFiler);

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
        BartsCodeableConceptHelper.applyCodeDisplayTxt(treatmentFunctionCodeCell, CernerCodeValueRef.TREATMENT_FUNCTION, encounterBuilder, EncounterBuilder.TAG_TREATMENT_FUNCTION, fhirResourceFiler);

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
        //fhirEncounter.addEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeResourceId.getResourceId().toString()));
        encounterBuilder.addEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeResourceId.getResourceId().toString()), episodeIdentiferCell);

        // Referrer
        CsvCell referrerPersonnelIdentifier = parser.getReferrerMillenniumPersonnelIdentifier();
        if (!referrerPersonnelIdentifier.isEmpty()) {
            ResourceId referrerPersonResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, referrerPersonnelIdentifier);
            if (referrerPersonResourceId != null) {
                encounterBuilder.addParticipant(csvHelper.createPractitionerReference(referrerPersonResourceId.getResourceId().toString()), EncounterParticipantType.REFERRER, referrerPersonnelIdentifier);
            } else {
                String valStr = "Practitioner Resource not found for Referrer-id " + parser.getReferrerMillenniumPersonnelIdentifier() + " in ENCNT record " + parser.getMillenniumEncounterIdentifier();
                LOG.debug(valStr);
                SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
            }
        }

        // responsible person
        CsvCell responsibleHCPCell = parser.getResponsibleHealthCareprovidingPersonnelIdentifier();
        if (!responsibleHCPCell.isEmpty()) {
            ResourceId respPersonResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, responsibleHCPCell);
            if (respPersonResourceId != null) {
                encounterBuilder.addParticipant(csvHelper.createPractitionerReference(respPersonResourceId.getResourceId().toString()), EncounterParticipantType.PRIMARY_PERFORMER, responsibleHCPCell);
            } else {
                String valStr = "Practitioner Resource not found for Personnel-id " + parser.getResponsibleHealthCareprovidingPersonnelIdentifier() + " in ENCNT record " + parser.getMillenniumEncounterIdentifier();
                LOG.debug(valStr);
                SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
            }
        }

        // registering person
        CsvCell registeringPersonnelIdentifierCell = parser.getRegisteringMillenniumPersonnelIdentifier();
        if (!registeringPersonnelIdentifierCell.isEmpty()) {
            ResourceId regPersonResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, registeringPersonnelIdentifierCell);
            if (regPersonResourceId != null) {
                encounterBuilder.addParticipant(csvHelper.createPractitionerReference(regPersonResourceId.getResourceId().toString()), EncounterParticipantType.PARTICIPANT, registeringPersonnelIdentifierCell);
            } else {
                String valStr = "Practitioner Resource not found for Personnel-id " + parser.getRegisteringMillenniumPersonnelIdentifier() + " in ENCNT record " + parser.getMillenniumEncounterIdentifier();
                LOG.debug(valStr);
                SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
            }
        }

        // Location
        CsvCell currentLocationCell = parser.getCurrentLocationIdentifier();
        if (!currentLocationCell.isEmpty()) {
            ResourceId locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, currentLocationCell.getString());
            if (locationResourceId != null) {
                encounterBuilder.addLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceId.getResourceId().toString()), currentLocationCell);
            } else {
                String valStr = "Location Resource not found for Location-id " + currentLocationCell.getString() + " in ENCNT record " + encounterIdCell.getString();
                LOG.debug(valStr);
                SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
            }
        }

        //cache our encounter details so subsequent transforms can use them
        csvHelper.cacheEncounterIds(encounterIdCell, (Encounter)encounterBuilder.getResource());

        LOG.debug("Save Encounter (PatId=" + patientUuid + ")(PersonId:" + parser.getMillenniumPersonIdentifier() + "):" + FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
    }



    /*
    *
     */
    private static Encounter.EncounterClass getEncounterClass(String millenniumCode) {
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
            return Encounter.EncounterClass.OTHER;
        }
    }

    /*
    *
     */
    private static Encounter.EncounterState getEncounterStatus(String millenniumCode) {
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
            return Encounter.EncounterState.NULL;
        }
    }

}
