package org.endeavourhealth.transform.barts.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.ENCNT;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.TransformRuntimeException;
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

        // Skip header line
        parser.nextRecord();

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

            if (episodeResourceId != null) {
                // TODO Check if Episode is to be removed (no more encounters for this episode)
            }

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

            // status

            //Reason
            CodeableConcept reasonForVisitText = CodeableConceptHelper.createCodeableConcept(parser.getReasonForVisitText());
            fhirEncounter.addReason(reasonForVisitText);

            // specialty

            // treatment function

            fhirEncounter.addEpisodeOfCare(ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeResourceId.getResourceId().toString()));

            // responsible person
            //fhirEncounter.addParticipant(csvHelper.createPractitionerReference("xxxxxxx"));

            // registering person
            //fhirEncounter.addParticipant(csvHelper.createPractitionerReference("xxxxxxx"));

            LOG.debug("Save Encounter (PatId=" + mrn + ")(PersonId:" + parser.getMillenniumPersonIdentifier() + "):" + FhirSerializationHelper.serializeResource(fhirEncounter));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirEncounter);
        }

    }

}
