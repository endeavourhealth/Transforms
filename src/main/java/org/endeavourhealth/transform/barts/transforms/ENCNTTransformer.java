package org.endeavourhealth.transform.barts.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsInternalIdDal;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.ENCNT;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.TransformRuntimeException;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        if (encounterResourceId == null && activeCell.getBoolean() == false) {
            // skip - encounter missing but set to delete so do nothing
            return;
        }
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterIdCell.getString());
        }

        // Get MRN (using person-id)
        if (internalIdDAL == null) {
            internalIdDAL = DalProvider.factoryInternalIdDal();
        }

        String mrn = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(),RdbmsInternalIdDal.IDTYPE_MRN_MILLENNIUM_PERS_ID, personIdCell.getString());
        if (mrn == null) {
            throw new TransformRuntimeException("MRN not found for PersonId " + parser.getMillenniumPersonIdentifier() + " in file " + parser.getFilePath());
        }

        // Save visit-id to encounter-id link
        internalIdDAL.upsertRecord(fhirResourceFiler.getServiceId(), RdbmsInternalIdDal.IDTYPE_ENCOUNTER_ID_VISIT_ID, visitIdCell.getString(), encounterIdCell.getString());

        // Episode resource id
        ResourceId episodeResourceId = getEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCell.getString());

        // Organisation - only used if placeholder patient resource is created
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Patient
        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(personIdCell.getString()))};
        ResourceId patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, mrn, null, null,null, null, null, organisationResourceId, null, patientIdentifier, null, null, null);

        //Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT , "clinical coding")};

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(encounterResourceId.getResourceId().toString(), encounterIdCell);

        if (activeCell.getBoolean() == false) {
            encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()), personIdCell);

            LOG.debug("Delete Encounter (PatId=" + personIdCell.getString() + "):" + FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
        } else {
            if (episodeResourceId == null) {
                episodeResourceId = createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCell.getString());

                EpisodeOfCare fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setStatus(EpisodeOfCare.EpisodeOfCareStatus.NULL);
                fhirEpisodeOfCare.setManagingOrganization(ReferenceHelper.createReference(ResourceType.Organization, organisationResourceId.getResourceId().toString()));
                fhirEpisodeOfCare.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
                savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirEpisodeOfCare);
            }

            //fhirEncounter.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ENCOUNTER));
            // Handled in builder now

            // Identifiers
            Identifier identifier;
            identifier = new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID).setValue(finIdCell.getString());
            encounterBuilder.addIdentifier(identifier, finIdCell);

            identifier = new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_VISIT_NO_EPISODE_ID).setValue(visitIdCell.getString());
            encounterBuilder.addIdentifier(identifier, visitIdCell);

            identifier = new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_ENCOUNTER_ID).setValue(encounterIdCell.getString());
            encounterBuilder.addIdentifier(identifier, encounterIdCell);

            // Patient
            //fhirEncounter.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
            encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()), personIdCell);

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
            if (!encounterTypeCodeCell.isEmpty()) {
                CernerCodeValueRef ret = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                                                                CernerCodeValueRef.PERSONNEL_SPECIALITY,
                                                                encounterTypeCodeCell.getLong(),
                                                                fhirResourceFiler.getServiceId());

                String encounterDispTxt;
                if (ret != null) {
                    encounterDispTxt = ret.getCodeDispTxt();
                } else {
                    encounterDispTxt = "??Unknown encounter type " + encounterTypeCodeCell.getString();
                    // LOG.warn("Code not found in Code Value lookup:" + encounterDispTxt);
                }
                CodeableConcept fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.IDENTIFIER_SYSTEM_BARTS_SPECIALTY, encounterDispTxt, encounterTypeCodeCell.getString());
                encounterBuilder.addExtension(FhirExtensionUri.ENCOUNTER_SPECIALTY, fhirCodeableConcept, encounterTypeCodeCell);
            }

            // treatment function
            if (!treatmentFunctionCodeCell.isEmpty()) {
                CernerCodeValueRef ret = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                                                                CernerCodeValueRef.TREATMENT_FUNCTION,
                                                                treatmentFunctionCodeCell.getLong(),
                                                                fhirResourceFiler.getServiceId());

                String treatFuncDispTxt;
                if (ret != null) {
                    treatFuncDispTxt = ret.getCodeDispTxt();
                } else {
                    treatFuncDispTxt = "??Unknown treatment function type " + treatmentFunctionCodeCell.getString();
                    // LOG.warn("Code not found in Code Value lookup:" + treatFuncDispTxt);
                }
                CodeableConcept fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.IDENTIFIER_SYSTEM_BARTS_TREATMENT_FUNCTION, treatFuncDispTxt, treatmentFunctionCodeCell.getString());
                encounterBuilder.addExtension(FhirExtensionUri.ENCOUNTER_TREATMENT_FUNCTION, fhirCodeableConcept);
            }

            // EpisodeOfCare
            //fhirEncounter.addEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeResourceId.getResourceId().toString()));
            encounterBuilder.addEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeResourceId.getResourceId().toString()), episodeIdentiferCell);

            // Referrer
            CsvCell referrerPersonnelIdentifier = parser.getReferrerMillenniumPersonnelIdentifier();
            if (!referrerPersonnelIdentifier.isEmpty()) {
                ResourceId referrerPersonResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, referrerPersonnelIdentifier.getString());
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
                ResourceId respPersonResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, responsibleHCPCell.getString());
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
                ResourceId regPersonResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, registeringPersonnelIdentifierCell.getString());
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

            LOG.debug("Save Encounter (PatId=" + mrn + ")(PersonId:" + parser.getMillenniumPersonIdentifier() + "):" + FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
        }

    }

    /*
    public static void createEncounter(ENCNT parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        // Encounter resource id
        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getMillenniumEncounterIdentifier());
        if (encounterResourceId == null && parser.isActive() == false) {
            // skip - encounter missing but set to delete so do nothing
            return;
        }
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getMillenniumEncounterIdentifier());
        }

        // Get MRN (using person-id)
        if (internalIdDAL == null) {
            internalIdDAL = DalProvider.factoryInternalIdDal();
            cernerCodeValueRefDAL = DalProvider.factoryCernerCodeValueRefDal();
        }
        String mrn = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(),"???type???", parser.getMillenniumPersonIdentifier());
        if (mrn == null) {
            throw new TransformRuntimeException("MRN not found for PersonId " + parser.getMillenniumPersonIdentifier() + " in file " + parser.getFilePath());
        }

        // Save visit-id to encounter-id link
        internalIdDAL.upsertRecord(fhirResourceFiler.getServiceId(),"???type???", parser.getMilleniumSourceIdentifierForVisit(), parser.getMillenniumEncounterIdentifier());

        // Episode resource id
        ResourceId episodeResourceId = getEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEpisodeIdentifier());

        // Organisation - only used if placeholder patient resource is created
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Patient
        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(parser.getMillenniumPersonIdentifier()))};
        ResourceId patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, mrn, null, null,null, null, null, organisationResourceId, null, patientIdentifier, null, null, null);

        //Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT , "clinical coding")};

        Encounter fhirEncounter = new Encounter();
        fhirEncounter.setId(encounterResourceId.getResourceId().toString());

        if (parser.isActive() == false) {
            fhirEncounter.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

            LOG.debug("Delete Encounter (PatId=" + parser.getMillenniumPersonIdentifier() + "):" + FhirSerializationHelper.serializeResource(fhirEncounter));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirEncounter);
        } else {
            if (episodeResourceId == null) {
                episodeResourceId = createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEpisodeIdentifier());

                EpisodeOfCare fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setStatus(EpisodeOfCare.EpisodeOfCareStatus.NULL);
                fhirEpisodeOfCare.setManagingOrganization(ReferenceHelper.createReference(ResourceType.Organization, organisationResourceId.getResourceId().toString()));
                fhirEpisodeOfCare.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));
                savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirEpisodeOfCare);
            }

            fhirEncounter.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ENCOUNTER));

            fhirEncounter.addIdentifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID).setValue(parser.getMillenniumFinancialNumberIdentifier());
            fhirEncounter.addIdentifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_VISIT_NO_EPISODE_ID).setValue(parser.getMilleniumSourceIdentifierForVisit());
            fhirEncounter.addIdentifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_ENCOUNTER_ID).setValue(parser.getMillenniumEncounterIdentifier());

            fhirEncounter.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

            // class
            fhirEncounter.setClass_(getEncounterClass(parser.getEncounterTypeMillenniumCode()));

            // status
            fhirEncounter.setStatus(getEncounterStatus(parser.getEncounterStatusMillenniumCode()));

            //Reason
            CodeableConcept reasonForVisitText = CodeableConceptHelper.createCodeableConcept(parser.getReasonForVisitText());
            fhirEncounter.addReason(reasonForVisitText);

            // specialty
            if (!Strings.isNullOrEmpty(parser.getEncounterTypeMillenniumCode())) {
                CernerCodeValueRef ret = cernerCodeValueRefDAL.getCodeFromCodeSet(RdbmsCernerCodeValueRefDal.PERSONNEL_SPECIALITY, Long.valueOf(parser.getEncounterTypeMillenniumCode()), fhirResourceFiler.getServiceId());
                CodeableConcept fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.IDENTIFIER_SYSTEM_BARTS_SPECIALTY, ret.getCodeDispTxt(), parser.getEncounterTypeMillenniumCode());
                fhirEncounter.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.ENCOUNTER_SPECIALTY, fhirCodeableConcept));
            }

            // treatment function
            //
            if (!Strings.isNullOrEmpty(parser.getCurrentTreatmentFunctionMillenniumCode())) {
                CernerCodeValueRef ret = cernerCodeValueRefDAL.getCodeFromCodeSet(RdbmsCernerCodeValueRefDal.TREATMENT_FUNCTION, Long.valueOf(parser.getCurrentTreatmentFunctionMillenniumCode()), fhirResourceFiler.getServiceId());
                CodeableConcept fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(FhirUri.IDENTIFIER_SYSTEM_BARTS_TREATMENT_FUNCTION, ret.getCodeDispTxt(), parser.getCurrentTreatmentFunctionMillenniumCode());
                fhirEncounter.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.ENCOUNTER_TREATMENT_FUNCTION, fhirCodeableConcept));
            }

            // EpisodeOfCare
            fhirEncounter.addEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeResourceId.getResourceId().toString()));

            // Referrer
            if (!Strings.isNullOrEmpty(parser.getReferrerMillenniumPersonnelIdentifier())) {
                ResourceId referrerPersonResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getReferrerMillenniumPersonnelIdentifier());
                if (referrerPersonResourceId != null) {
                    Encounter.EncounterParticipantComponent fhirParticipant = fhirEncounter.addParticipant();
                    fhirParticipant.addType(CodeableConceptHelper.createCodeableConcept(EncounterParticipantType.REFERRER));
                    fhirParticipant.setIndividual(csvHelper.createPractitionerReference(referrerPersonResourceId.getResourceId().toString()));
                    fhirEncounter.addParticipant(fhirParticipant);
                } else {
                    String valStr = "Practitioner Resource not found for Referrer-id " + parser.getReferrerMillenniumPersonnelIdentifier() + " in ENCNT record " + parser.getMillenniumEncounterIdentifier();
                    LOG.debug(valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            }

            // responsible person
            ResourceId respPersonResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getResponsibleHealthCareprovidingPersonnelIdentifier());
            if (respPersonResourceId != null) {
                Encounter.EncounterParticipantComponent fhirParticipant = fhirEncounter.addParticipant();
                fhirParticipant.addType(CodeableConceptHelper.createCodeableConcept(EncounterParticipantType.PRIMARY_PERFORMER));
                fhirParticipant.setIndividual(csvHelper.createPractitionerReference(respPersonResourceId.getResourceId().toString()));
                fhirEncounter.addParticipant(fhirParticipant);
            } else {
                String valStr = "Practitioner Resource not found for Personnel-id " + parser.getResponsibleHealthCareprovidingPersonnelIdentifier() + " in ENCNT record " + parser.getMillenniumEncounterIdentifier();
                LOG.debug(valStr);
                SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
            }

            // registering person
            ResourceId regPersonResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getRegisteringMillenniumPersonnelIdentifier());
            if (regPersonResourceId != null) {
                Encounter.EncounterParticipantComponent fhirParticipant = fhirEncounter.addParticipant();
                fhirParticipant.addType(CodeableConceptHelper.createCodeableConcept(EncounterParticipantType.PARTICIPANT));
                fhirParticipant.setIndividual(csvHelper.createPractitionerReference(regPersonResourceId.getResourceId().toString()));
                fhirEncounter.addParticipant(fhirParticipant);
            } else {
                String valStr = "Practitioner Resource not found for Personnel-id " + parser.getRegisteringMillenniumPersonnelIdentifier() + " in ENCNT record " + parser.getMillenniumEncounterIdentifier();
                LOG.debug(valStr);
                SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
            }

            // Location
            if (!Strings.isNullOrEmpty(parser.getCurrentLocationIdentifier())) {
                ResourceId locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getCurrentLocationIdentifier());
                if (locationResourceId != null) {
                    fhirEncounter.addLocation().setLocation(ReferenceHelper.createReference(ResourceType.Location, locationResourceId.getResourceId().toString()));
                } else {
                    String valStr = "Location Resource not found for Location-id " + parser.getCurrentLocationIdentifier() + " in ENCNT record " + parser.getMillenniumEncounterIdentifier();
                    LOG.debug(valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            }

            LOG.debug("Save Encounter (PatId=" + mrn + ")(PersonId:" + parser.getMillenniumPersonIdentifier() + "):" + FhirSerializationHelper.serializeResource(fhirEncounter));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirEncounter);
        }

    }*/


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
