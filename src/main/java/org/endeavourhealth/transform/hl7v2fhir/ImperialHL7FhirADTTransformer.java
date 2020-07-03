package org.endeavourhealth.transform.hl7v2fhir;

import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v23.datatype.*;
import ca.uhn.hl7v2.model.v23.message.*;
import ca.uhn.hl7v2.model.v23.segment.MSH;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.endeavourhealth.transform.hl7v2fhir.transforms.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ImperialHL7FhirADTTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ImperialHL7FhirADTTransformer.class);

    /**
     *
     * @param exchangeBody
     * @param fhirResourceFiler
     * @param version
     * @param hapiMsg
     * @throws Exception
     */
    public static void transform(String exchangeBody, FhirResourceFiler fhirResourceFiler, String version, Message hapiMsg) throws Exception {

        String msgType = (hapiMsg.printStructure()).substring(0,7);
        ImperialHL7Helper imperialHL7Helper = new ImperialHL7Helper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(),
                fhirResourceFiler.getExchangeId(), null, null);

        if("ADT_A01".equalsIgnoreCase(msgType)) {
            ADT_A01 adtMsg = (ADT_A01) hapiMsg;

            //MessageHeader
            MessageHeader fhirMessageHeader = null;
            fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
            //MessageHeader

            //Organization
            Organization fhirOrganization = null;
            fhirOrganization = new Organization();
            fhirOrganization = OrganizationTransformer.transformPV1ToOrganization(fhirOrganization);
            //Organization

            //Practitioner
            Practitioner fhirPractitioner = null;
            fhirPractitioner = new Practitioner();
            fhirPractitioner = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1(), fhirPractitioner);
            fhirPractitioner.addPractitionerRole().setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(fhirPractitioner);
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = LocationTransformer.transformPV1ToOrgLocation(fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
            fhirLocationPatientAssLoc.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderPatientAssLoc = new LocationBuilder(fhirLocationPatientAssLoc);
            fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
            //LocationPatientAssLoc

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            fhirResourceFiler.saveAdminResource(null, organizationBuilder);
            //Organization

            //Patient
            CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
            String patientGuid = String.valueOf(patientIdList[0].getID());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), fhirPatient);

            if (newPatient) {
                fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirPatient.setManagingOrganization(organizationReference);
                fhirPatient.addCareProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirPatient.addCareProvider(practitionerReference);
            }

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            if(newPatient) {
                fhirResourceFiler.savePatientResource(null, true, patientBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            }
            //Patient

            //EpisodeOfCare
            boolean newEpisodeOfCare = false;
            EpisodeOfCare fhirEpisodeOfCare = null;

            /*TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());*/
            String visitNum = String.valueOf(adtMsg.getPV1().getVisitNumber().getID());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
            if(fhirEpisodeOfCare == null) {
                fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setId(sourceEpisodeId);
                newEpisodeOfCare = true;
            }
            fhirEpisodeOfCare = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
            if (newPatient) {
                fhirEpisodeOfCare.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEpisodeOfCare.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirEpisodeOfCare.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEpisodeOfCare.setCareManager(practitionerReference);
            }

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            if(newEpisodeOfCare) {
                fhirResourceFiler.savePatientResource(null, true, episodeOfCareBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, episodeOfCareBuilder);
            }
            //EpisodeOfCare

            //Encounter
            boolean newEncounter = false;
            Encounter fhirEncounter = null;
            //String locallyUniqueResourceEncounterId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEncounter = (Encounter) imperialHL7Helper.retrieveResource(visitNum, ResourceType.Encounter);
            if (fhirEncounter == null) {
                fhirEncounter = new Encounter();
                fhirEncounter.setId(visitNum);
                newEncounter = true;
            }
            EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter, fhirResourceFiler, imperialHL7Helper, msgType, patientGuid);
            /*if (newPatient) {
                fhirEncounter.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEncounter.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
                fhirEncounter.addEpisodeOfCare(ImperialHL7Helper.createReference(ResourceType.EpisodeOfCare, fhirEpisodeOfCare.getId()));
                fhirEncounter.addParticipant().setIndividual(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
                fhirEncounter.addLocation().setLocation(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationPatientAssLoc.getId()));
                fhirEncounter.setServiceProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEncounter.setPatient(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEncounter.setServiceProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEncounter.addExtension().setValue(practitionerReference);
                fhirEncounter.addParticipant().setIndividual(practitionerReference);

                Reference episodeOfCareReference = imperialHL7Helper.createEpisodeOfCareReference(fhirEpisodeOfCare.getId());
                episodeOfCareReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeOfCareReference, fhirResourceFiler);
                fhirEncounter.addEpisodeOfCare(episodeOfCareReference);

                Reference locationReference = imperialHL7Helper.createLocationReference(fhirLocationPatientAssLoc.getId());
                locationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(locationReference, fhirResourceFiler);
                fhirEncounter.addLocation().setLocation(locationReference);
            }

            EncounterBuilder encounterBuilder = new EncounterBuilder(fhirEncounter);
            if(newEncounter) {
                fhirResourceFiler.savePatientResource(null, true, encounterBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
            }*/
            //Encounter

        } else if("ADT_A02".equalsIgnoreCase(msgType)) {
            ADT_A02 adtMsg = (ADT_A02) hapiMsg;

            //MessageHeader
            MessageHeader fhirMessageHeader = null;
            fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
            //MessageHeader

            //Organization
            Organization fhirOrganization = null;
            fhirOrganization = new Organization();
            fhirOrganization = OrganizationTransformer.transformPV1ToOrganization(fhirOrganization);
            //Organization

            //Practitioner
            Practitioner fhirPractitioner = null;
            fhirPractitioner = new Practitioner();
            fhirPractitioner = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1(), fhirPractitioner);
            fhirPractitioner.addPractitionerRole().setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(fhirPractitioner);
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = LocationTransformer.transformPV1ToOrgLocation(fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
            fhirLocationPatientAssLoc.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderPatientAssLoc = new LocationBuilder(fhirLocationPatientAssLoc);
            fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
            //LocationPatientAssLoc

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            fhirResourceFiler.saveAdminResource(null, organizationBuilder);
            //Organization

            //Patient
            CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
            String patientGuid = String.valueOf(patientIdList[0].getID());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), fhirPatient);

            if (newPatient) {
                fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirPatient.setManagingOrganization(organizationReference);
                fhirPatient.addCareProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirPatient.addCareProvider(practitionerReference);
            }

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            if(newPatient) {
                fhirResourceFiler.savePatientResource(null, true, patientBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            }
            //Patient

            //EpisodeOfCare
            boolean newEpisodeOfCare = false;
            EpisodeOfCare fhirEpisodeOfCare = null;

            /*TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());*/
            String visitNum = String.valueOf(adtMsg.getPV1().getVisitNumber().getID());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
            if(fhirEpisodeOfCare == null) {
                fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setId(sourceEpisodeId);
                newEpisodeOfCare = true;
            }
            fhirEpisodeOfCare = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
            if (newPatient) {
                fhirEpisodeOfCare.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEpisodeOfCare.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirEpisodeOfCare.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEpisodeOfCare.setCareManager(practitionerReference);
            }

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            if(newEpisodeOfCare) {
                fhirResourceFiler.savePatientResource(null, true, episodeOfCareBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, episodeOfCareBuilder);
            }
            //EpisodeOfCare

            //Encounter
            boolean newEncounter = false;
            Encounter fhirEncounter = null;
            //String locallyUniqueResourceEncounterId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEncounter = (Encounter) imperialHL7Helper.retrieveResource(visitNum, ResourceType.Encounter);
            if (fhirEncounter == null) {
                fhirEncounter = new Encounter();
                fhirEncounter.setId(visitNum);
                newEncounter = true;
            }
            EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter, fhirResourceFiler, imperialHL7Helper, msgType, patientGuid);
            /*if (newPatient) {
                fhirEncounter.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEncounter.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
                fhirEncounter.addEpisodeOfCare(ImperialHL7Helper.createReference(ResourceType.EpisodeOfCare, fhirEpisodeOfCare.getId()));
                fhirEncounter.addParticipant().setIndividual(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
                fhirEncounter.addLocation().setLocation(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationPatientAssLoc.getId()));
                fhirEncounter.setServiceProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEncounter.setPatient(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEncounter.setServiceProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEncounter.addExtension().setValue(practitionerReference);
                fhirEncounter.addParticipant().setIndividual(practitionerReference);

                Reference episodeOfCareReference = imperialHL7Helper.createEpisodeOfCareReference(fhirEpisodeOfCare.getId());
                episodeOfCareReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeOfCareReference, fhirResourceFiler);
                fhirEncounter.addEpisodeOfCare(episodeOfCareReference);

                Reference locationReference = imperialHL7Helper.createLocationReference(fhirLocationPatientAssLoc.getId());
                locationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(locationReference, fhirResourceFiler);
                fhirEncounter.addLocation().setLocation(locationReference);
            }

            EncounterBuilder encounterBuilder = new EncounterBuilder(fhirEncounter);
            if(newEncounter) {
                fhirResourceFiler.savePatientResource(null, true, encounterBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
            }*/
            //Encounter

        } else if("ADT_A03".equalsIgnoreCase(msgType)) {
            ADT_A03 adtMsg = (ADT_A03) hapiMsg;

            //MessageHeader
            MessageHeader fhirMessageHeader = null;
            fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
            //MessageHeader

            //Organization
            Organization fhirOrganization = null;
            fhirOrganization = new Organization();
            fhirOrganization = OrganizationTransformer.transformPV1ToOrganization(fhirOrganization);
            //Organization

            //Practitioner
            Practitioner fhirPractitioner = null;
            fhirPractitioner = new Practitioner();
            fhirPractitioner = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1(), fhirPractitioner);
            fhirPractitioner.addPractitionerRole().setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(fhirPractitioner);
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = LocationTransformer.transformPV1ToOrgLocation(fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
            fhirLocationPatientAssLoc.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderPatientAssLoc = new LocationBuilder(fhirLocationPatientAssLoc);
            fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
            //LocationPatientAssLoc

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            fhirResourceFiler.saveAdminResource(null, organizationBuilder);
            //Organization

            //Patient
            CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
            String patientGuid = String.valueOf(patientIdList[0].getID());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), fhirPatient);

            if (newPatient) {
                fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirPatient.setManagingOrganization(organizationReference);
                fhirPatient.addCareProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirPatient.addCareProvider(practitionerReference);
            }

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            if(newPatient) {
                fhirResourceFiler.savePatientResource(null, true, patientBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            }
            //Patient

            //EpisodeOfCare
            boolean newEpisodeOfCare = false;
            EpisodeOfCare fhirEpisodeOfCare = null;

            /*TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());*/
            String visitNum = String.valueOf(adtMsg.getPV1().getVisitNumber().getID());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
            if(fhirEpisodeOfCare == null) {
                fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setId(sourceEpisodeId);
                newEpisodeOfCare = true;
            }
            fhirEpisodeOfCare = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
            if (newPatient) {
                fhirEpisodeOfCare.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEpisodeOfCare.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirEpisodeOfCare.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEpisodeOfCare.setCareManager(practitionerReference);
            }

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            if(newEpisodeOfCare) {
                fhirResourceFiler.savePatientResource(null, true, episodeOfCareBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, episodeOfCareBuilder);
            }
            //EpisodeOfCare

            //Encounter
            boolean newEncounter = false;
            Encounter fhirEncounter = null;
            //String locallyUniqueResourceEncounterId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEncounter = (Encounter) imperialHL7Helper.retrieveResource(visitNum, ResourceType.Encounter);
            if (fhirEncounter == null) {
                fhirEncounter = new Encounter();
                fhirEncounter.setId(visitNum);
                newEncounter = true;
            }
            EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter, fhirResourceFiler, imperialHL7Helper, msgType, patientGuid);
            /*if (newPatient) {
                fhirEncounter.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEncounter.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
                fhirEncounter.addEpisodeOfCare(ImperialHL7Helper.createReference(ResourceType.EpisodeOfCare, fhirEpisodeOfCare.getId()));
                fhirEncounter.addParticipant().setIndividual(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
                fhirEncounter.addLocation().setLocation(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationPatientAssLoc.getId()));
                fhirEncounter.setServiceProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEncounter.setPatient(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEncounter.setServiceProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEncounter.addExtension().setValue(practitionerReference);
                fhirEncounter.addParticipant().setIndividual(practitionerReference);

                Reference episodeOfCareReference = imperialHL7Helper.createEpisodeOfCareReference(fhirEpisodeOfCare.getId());
                episodeOfCareReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeOfCareReference, fhirResourceFiler);
                fhirEncounter.addEpisodeOfCare(episodeOfCareReference);

                Reference locationReference = imperialHL7Helper.createLocationReference(fhirLocationPatientAssLoc.getId());
                locationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(locationReference, fhirResourceFiler);
                fhirEncounter.addLocation().setLocation(locationReference);
            }

            EncounterBuilder encounterBuilder = new EncounterBuilder(fhirEncounter);
            if(newEncounter) {
                fhirResourceFiler.savePatientResource(null, true, encounterBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
            }*/
            //Encounter

        } else if("ADT_A11".equalsIgnoreCase(msgType)) {
            ADT_A11 adtMsg = (ADT_A11) hapiMsg;

            //MessageHeader
            MessageHeader fhirMessageHeader = null;
            fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
            //MessageHeader

            //Organization
            Organization fhirOrganization = null;
            fhirOrganization = new Organization();
            fhirOrganization = OrganizationTransformer.transformPV1ToOrganization(fhirOrganization);
            //Organization

            //Practitioner
            Practitioner fhirPractitioner = null;
            fhirPractitioner = new Practitioner();
            fhirPractitioner = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1(), fhirPractitioner);
            fhirPractitioner.addPractitionerRole().setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(fhirPractitioner);
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = LocationTransformer.transformPV1ToOrgLocation(fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
            fhirLocationPatientAssLoc.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderPatientAssLoc = new LocationBuilder(fhirLocationPatientAssLoc);
            fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
            //LocationPatientAssLoc

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            fhirResourceFiler.saveAdminResource(null, organizationBuilder);
            //Organization

            //Patient
            CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
            String patientGuid = String.valueOf(patientIdList[0].getID());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), fhirPatient);

            if (newPatient) {
                fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirPatient.setManagingOrganization(organizationReference);
                fhirPatient.addCareProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirPatient.addCareProvider(practitionerReference);
            }

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            if(newPatient) {
                fhirResourceFiler.savePatientResource(null, true, patientBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            }
            //Patient

            //EpisodeOfCare
            boolean newEpisodeOfCare = false;
            EpisodeOfCare fhirEpisodeOfCare = null;

            /*TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());*/
            String visitNum = String.valueOf(adtMsg.getPV1().getVisitNumber().getID());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
            if(fhirEpisodeOfCare == null) {
                fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setId(sourceEpisodeId);
                newEpisodeOfCare = true;
            }
            fhirEpisodeOfCare = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
            if (newPatient) {
                fhirEpisodeOfCare.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEpisodeOfCare.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirEpisodeOfCare.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEpisodeOfCare.setCareManager(practitionerReference);
            }

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            if(newEpisodeOfCare) {
                fhirResourceFiler.savePatientResource(null, true, episodeOfCareBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, episodeOfCareBuilder);
            }
            //EpisodeOfCare

            //Encounter
            Encounter fhirEncounter = null;
            fhirEncounter = (Encounter) imperialHL7Helper.retrieveResource(visitNum, ResourceType.Encounter);

            EncounterBuilder encounterBuilder = new EncounterBuilder(fhirEncounter);
            fhirResourceFiler.deletePatientResource(null, false, encounterBuilder);
            //Encounter

        } else if("ADT_A12".equalsIgnoreCase(msgType)) {
            ADT_A12 adtMsg = (ADT_A12) hapiMsg;

            //MessageHeader
            MessageHeader fhirMessageHeader = null;
            fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
            //MessageHeader

            //Organization
            Organization fhirOrganization = null;
            fhirOrganization = new Organization();
            fhirOrganization = OrganizationTransformer.transformPV1ToOrganization(fhirOrganization);
            //Organization

            //Practitioner
            Practitioner fhirPractitioner = null;
            fhirPractitioner = new Practitioner();
            fhirPractitioner = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1(), fhirPractitioner);
            fhirPractitioner.addPractitionerRole().setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(fhirPractitioner);
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = LocationTransformer.transformPV1ToOrgLocation(fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
            fhirLocationPatientAssLoc.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderPatientAssLoc = new LocationBuilder(fhirLocationPatientAssLoc);
            fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
            //LocationPatientAssLoc

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            fhirResourceFiler.saveAdminResource(null, organizationBuilder);
            //Organization

            //Patient
            CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
            String patientGuid = String.valueOf(patientIdList[0].getID());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), fhirPatient);

            if (newPatient) {
                fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirPatient.setManagingOrganization(organizationReference);
                fhirPatient.addCareProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirPatient.addCareProvider(practitionerReference);
            }

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            if(newPatient) {
                fhirResourceFiler.savePatientResource(null, true, patientBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            }
            //Patient

            //EpisodeOfCare
            boolean newEpisodeOfCare = false;
            EpisodeOfCare fhirEpisodeOfCare = null;

            /*TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());*/
            String visitNum = String.valueOf(adtMsg.getPV1().getVisitNumber().getID());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
            if(fhirEpisodeOfCare == null) {
                fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setId(sourceEpisodeId);
                newEpisodeOfCare = true;
            }
            fhirEpisodeOfCare = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
            if (newPatient) {
                fhirEpisodeOfCare.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEpisodeOfCare.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirEpisodeOfCare.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEpisodeOfCare.setCareManager(practitionerReference);
            }

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            if(newEpisodeOfCare) {
                fhirResourceFiler.savePatientResource(null, true, episodeOfCareBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, episodeOfCareBuilder);
            }
            //EpisodeOfCare

            //Encounter
            Encounter fhirEncounter = null;
            fhirEncounter = (Encounter) imperialHL7Helper.retrieveResource(visitNum, ResourceType.Encounter);

            EncounterBuilder encounterBuilder = new EncounterBuilder(fhirEncounter);
            fhirResourceFiler.deletePatientResource(null, false, encounterBuilder);
            //Encounter

        } else if("ADT_A13".equalsIgnoreCase(msgType)) {
            ADT_A13 adtMsg = (ADT_A13) hapiMsg;

            //MessageHeader
            MessageHeader fhirMessageHeader = null;
            fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
            //MessageHeader

            //Organization
            Organization fhirOrganization = null;
            fhirOrganization = new Organization();
            fhirOrganization = OrganizationTransformer.transformPV1ToOrganization(fhirOrganization);
            //Organization

            //Practitioner
            Practitioner fhirPractitioner = null;
            fhirPractitioner = new Practitioner();
            fhirPractitioner = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1(), fhirPractitioner);
            fhirPractitioner.addPractitionerRole().setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(fhirPractitioner);
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = LocationTransformer.transformPV1ToOrgLocation(fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
            fhirLocationPatientAssLoc.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderPatientAssLoc = new LocationBuilder(fhirLocationPatientAssLoc);
            fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
            //LocationPatientAssLoc

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            fhirResourceFiler.saveAdminResource(null, organizationBuilder);
            //Organization

            //Patient
            CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
            String patientGuid = String.valueOf(patientIdList[0].getID());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), fhirPatient);

            if (newPatient) {
                fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirPatient.setManagingOrganization(organizationReference);
                fhirPatient.addCareProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirPatient.addCareProvider(practitionerReference);
            }

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            if(newPatient) {
                fhirResourceFiler.savePatientResource(null, true, patientBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            }
            //Patient

            //EpisodeOfCare
            boolean newEpisodeOfCare = false;
            EpisodeOfCare fhirEpisodeOfCare = null;

            /*TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());*/
            String visitNum = String.valueOf(adtMsg.getPV1().getVisitNumber().getID());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
            if(fhirEpisodeOfCare == null) {
                fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setId(sourceEpisodeId);
                newEpisodeOfCare = true;
            }
            fhirEpisodeOfCare = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
            if (newPatient) {
                fhirEpisodeOfCare.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEpisodeOfCare.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirEpisodeOfCare.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEpisodeOfCare.setCareManager(practitionerReference);
            }

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            if(newEpisodeOfCare) {
                fhirResourceFiler.savePatientResource(null, true, episodeOfCareBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, episodeOfCareBuilder);
            }
            //EpisodeOfCare

            //Encounter
            Encounter fhirEncounter = null;
            fhirEncounter = (Encounter) imperialHL7Helper.retrieveResource(visitNum, ResourceType.Encounter);

            EncounterBuilder encounterBuilder = new EncounterBuilder(fhirEncounter);
            fhirResourceFiler.deletePatientResource(null, false, encounterBuilder);
            //Encounter

        } else if("ADT_A08".equalsIgnoreCase(msgType)) {
            ADT_A08 adtMsg = (ADT_A08) hapiMsg;

            //MessageHeader
            MessageHeader fhirMessageHeader = null;
            fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
            //MessageHeader

            //Organization
            Organization fhirOrganization = null;
            fhirOrganization = new Organization();
            fhirOrganization = OrganizationTransformer.transformPV1ToOrganization(fhirOrganization);
            //Organization

            //Practitioner
            Practitioner fhirPractitioner = null;
            fhirPractitioner = new Practitioner();
            fhirPractitioner = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1(), fhirPractitioner);
            fhirPractitioner.addPractitionerRole().setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(fhirPractitioner);
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = LocationTransformer.transformPV1ToOrgLocation(fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
            fhirLocationPatientAssLoc.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderPatientAssLoc = new LocationBuilder(fhirLocationPatientAssLoc);
            fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
            //LocationPatientAssLoc

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            fhirResourceFiler.saveAdminResource(null, organizationBuilder);
            //Organization

            //Patient
            CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
            String patientGuid = String.valueOf(patientIdList[0].getID());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), fhirPatient);

            if (newPatient) {
                fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirPatient.setManagingOrganization(organizationReference);
                fhirPatient.addCareProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirPatient.addCareProvider(practitionerReference);
            }

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            if(newPatient) {
                fhirResourceFiler.savePatientResource(null, true, patientBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            }
            //Patient

            //EpisodeOfCare
            boolean newEpisodeOfCare = false;
            EpisodeOfCare fhirEpisodeOfCare = null;

            /*TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());*/
            String visitNum = String.valueOf(adtMsg.getPV1().getVisitNumber().getID());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
            if(fhirEpisodeOfCare == null) {
                fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setId(sourceEpisodeId);
                newEpisodeOfCare = true;
            }
            fhirEpisodeOfCare = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
            if (newPatient) {
                fhirEpisodeOfCare.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEpisodeOfCare.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirEpisodeOfCare.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEpisodeOfCare.setCareManager(practitionerReference);
            }

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            if(newEpisodeOfCare) {
                fhirResourceFiler.savePatientResource(null, true, episodeOfCareBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, episodeOfCareBuilder);
            }
            //EpisodeOfCare

            //Encounter
            boolean newEncounter = false;
            Encounter fhirEncounter = null;
            //String locallyUniqueResourceEncounterId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEncounter = (Encounter) imperialHL7Helper.retrieveResource(visitNum, ResourceType.Encounter);
            if (fhirEncounter == null) {
                fhirEncounter = new Encounter();
                fhirEncounter.setId(visitNum);
                newEncounter = true;
            }
            EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter, fhirResourceFiler, imperialHL7Helper, msgType, patientGuid);
            /*if (newPatient) {
                fhirEncounter.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEncounter.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
                fhirEncounter.addEpisodeOfCare(ImperialHL7Helper.createReference(ResourceType.EpisodeOfCare, fhirEpisodeOfCare.getId()));
                fhirEncounter.addParticipant().setIndividual(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
                fhirEncounter.addLocation().setLocation(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationPatientAssLoc.getId()));
                fhirEncounter.setServiceProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEncounter.setPatient(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEncounter.setServiceProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEncounter.addExtension().setValue(practitionerReference);
                fhirEncounter.addParticipant().setIndividual(practitionerReference);

                Reference episodeOfCareReference = imperialHL7Helper.createEpisodeOfCareReference(fhirEpisodeOfCare.getId());
                episodeOfCareReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeOfCareReference, fhirResourceFiler);
                fhirEncounter.addEpisodeOfCare(episodeOfCareReference);

                Reference locationReference = imperialHL7Helper.createLocationReference(fhirLocationPatientAssLoc.getId());
                locationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(locationReference, fhirResourceFiler);
                fhirEncounter.addLocation().setLocation(locationReference);
            }

            EncounterBuilder encounterBuilder = new EncounterBuilder(fhirEncounter);
            if(newEncounter) {
                fhirResourceFiler.savePatientResource(null, true, encounterBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
            }*/
            //Encounter

        } else if("ADT_A28".equalsIgnoreCase(msgType)) {
            ADT_A28 adtMsg = (ADT_A28) hapiMsg;

            //MessageHeader
            MessageHeader fhirMessageHeader = null;
            fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
            //MessageHeader

            //Organization
            Organization fhirOrganization = null;
            fhirOrganization = new Organization();
            fhirOrganization = OrganizationTransformer.transformPV1ToOrganization(fhirOrganization);
            //Organization

            //Practitioner
            Practitioner fhirPractitioner = null;
            fhirPractitioner = new Practitioner();
            fhirPractitioner = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1(), fhirPractitioner);
            fhirPractitioner.addPractitionerRole().setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(fhirPractitioner);
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = LocationTransformer.transformPV1ToOrgLocation(fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
            fhirLocationPatientAssLoc.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderPatientAssLoc = new LocationBuilder(fhirLocationPatientAssLoc);
            fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
            //LocationPatientAssLoc

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            fhirResourceFiler.saveAdminResource(null, organizationBuilder);
            //Organization

            //Patient
            CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
            String patientGuid = String.valueOf(patientIdList[0].getID());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), fhirPatient);

            if (newPatient) {
                fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirPatient.setManagingOrganization(organizationReference);
                fhirPatient.addCareProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirPatient.addCareProvider(practitionerReference);
            }

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            if(newPatient) {
                fhirResourceFiler.savePatientResource(null, true, patientBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            }
            //Patient

            //EpisodeOfCare
            boolean newEpisodeOfCare = false;
            EpisodeOfCare fhirEpisodeOfCare = null;

            /*TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());*/
            String visitNum = String.valueOf(adtMsg.getPV1().getVisitNumber().getID());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
            if(fhirEpisodeOfCare == null) {
                fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setId(sourceEpisodeId);
                newEpisodeOfCare = true;
            }
            fhirEpisodeOfCare = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
            if (newPatient) {
                fhirEpisodeOfCare.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEpisodeOfCare.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirEpisodeOfCare.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEpisodeOfCare.setCareManager(practitionerReference);
            }

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            if(newEpisodeOfCare) {
                fhirResourceFiler.savePatientResource(null, true, episodeOfCareBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, episodeOfCareBuilder);
            }
            //EpisodeOfCare

            //Encounter
            boolean newEncounter = false;
            Encounter fhirEncounter = null;
            //String locallyUniqueResourceEncounterId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEncounter = (Encounter) imperialHL7Helper.retrieveResource(visitNum, ResourceType.Encounter);
            if (fhirEncounter == null) {
                fhirEncounter = new Encounter();
                fhirEncounter.setId(visitNum);
                newEncounter = true;
            }
            EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter, fhirResourceFiler, imperialHL7Helper, msgType, patientGuid);
            /*if (newPatient) {
                fhirEncounter.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEncounter.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
                fhirEncounter.addEpisodeOfCare(ImperialHL7Helper.createReference(ResourceType.EpisodeOfCare, fhirEpisodeOfCare.getId()));
                fhirEncounter.addParticipant().setIndividual(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
                fhirEncounter.addLocation().setLocation(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationPatientAssLoc.getId()));
                fhirEncounter.setServiceProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEncounter.setPatient(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEncounter.setServiceProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEncounter.addExtension().setValue(practitionerReference);
                fhirEncounter.addParticipant().setIndividual(practitionerReference);

                Reference episodeOfCareReference = imperialHL7Helper.createEpisodeOfCareReference(fhirEpisodeOfCare.getId());
                episodeOfCareReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeOfCareReference, fhirResourceFiler);
                fhirEncounter.addEpisodeOfCare(episodeOfCareReference);

                Reference locationReference = imperialHL7Helper.createLocationReference(fhirLocationPatientAssLoc.getId());
                locationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(locationReference, fhirResourceFiler);
                fhirEncounter.addLocation().setLocation(locationReference);
            }

            EncounterBuilder encounterBuilder = new EncounterBuilder(fhirEncounter);
            if(newEncounter) {
                fhirResourceFiler.savePatientResource(null, true, encounterBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
            }*/
            //Encounter

        } else if("ADT_A31".equalsIgnoreCase(msgType)) {
            ADT_A31 adtMsg = (ADT_A31) hapiMsg;

            //MessageHeader
            MessageHeader fhirMessageHeader = null;
            fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
            //MessageHeader

            //Organization
            Organization fhirOrganization = null;
            fhirOrganization = new Organization();
            fhirOrganization = OrganizationTransformer.transformPV1ToOrganization(fhirOrganization);
            //Organization

            //Practitioner
            Practitioner fhirPractitioner = null;
            fhirPractitioner = new Practitioner();
            fhirPractitioner = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1(), fhirPractitioner);
            fhirPractitioner.addPractitionerRole().setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(fhirPractitioner);
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = LocationTransformer.transformPV1ToOrgLocation(fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
            fhirLocationPatientAssLoc.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderPatientAssLoc = new LocationBuilder(fhirLocationPatientAssLoc);
            fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
            //LocationPatientAssLoc

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            fhirResourceFiler.saveAdminResource(null, organizationBuilder);
            //Organization

            //Patient
            CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
            String patientGuid = String.valueOf(patientIdList[0].getID());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), fhirPatient);

            if (newPatient) {
                fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirPatient.setManagingOrganization(organizationReference);
                fhirPatient.addCareProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirPatient.addCareProvider(practitionerReference);
            }

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            if(newPatient) {
                fhirResourceFiler.savePatientResource(null, true, patientBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            }
            //Patient

            //EpisodeOfCare
            boolean newEpisodeOfCare = false;
            EpisodeOfCare fhirEpisodeOfCare = null;

            /*TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());*/
            String visitNum = String.valueOf(adtMsg.getPV1().getVisitNumber().getID());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
            if(fhirEpisodeOfCare == null) {
                fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setId(sourceEpisodeId);
                newEpisodeOfCare = true;
            }
            fhirEpisodeOfCare = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
            if (newPatient) {
                fhirEpisodeOfCare.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEpisodeOfCare.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirEpisodeOfCare.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEpisodeOfCare.setManagingOrganization(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEpisodeOfCare.setCareManager(practitionerReference);
            }

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            if(newEpisodeOfCare) {
                fhirResourceFiler.savePatientResource(null, true, episodeOfCareBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, episodeOfCareBuilder);
            }
            //EpisodeOfCare

            //Encounter
            boolean newEncounter = false;
            Encounter fhirEncounter = null;
            //String locallyUniqueResourceEncounterId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
            fhirEncounter = (Encounter) imperialHL7Helper.retrieveResource(visitNum, ResourceType.Encounter);
            if (fhirEncounter == null) {
                fhirEncounter = new Encounter();
                fhirEncounter.setId(visitNum);
                newEncounter = true;
            }
            EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter, fhirResourceFiler, imperialHL7Helper, msgType, patientGuid);
            /*if (newPatient) {
                fhirEncounter.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
                fhirEncounter.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
                fhirEncounter.addEpisodeOfCare(ImperialHL7Helper.createReference(ResourceType.EpisodeOfCare, fhirEpisodeOfCare.getId()));
                fhirEncounter.addParticipant().setIndividual(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
                fhirEncounter.addLocation().setLocation(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationPatientAssLoc.getId()));
                fhirEncounter.setServiceProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            } else {
                Reference patientReference = imperialHL7Helper.createPatientReference(fhirPatient.getId());
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                fhirEncounter.setPatient(patientReference);

                Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirEncounter.setServiceProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirEncounter.addExtension().setValue(practitionerReference);
                fhirEncounter.addParticipant().setIndividual(practitionerReference);

                Reference episodeOfCareReference = imperialHL7Helper.createEpisodeOfCareReference(fhirEpisodeOfCare.getId());
                episodeOfCareReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeOfCareReference, fhirResourceFiler);
                fhirEncounter.addEpisodeOfCare(episodeOfCareReference);

                Reference locationReference = imperialHL7Helper.createLocationReference(fhirLocationPatientAssLoc.getId());
                locationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(locationReference, fhirResourceFiler);
                fhirEncounter.addLocation().setLocation(locationReference);
            }

            EncounterBuilder encounterBuilder = new EncounterBuilder(fhirEncounter);
            if(newEncounter) {
                fhirResourceFiler.savePatientResource(null, true, encounterBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
            }*/
            //Encounter

        } else if("ADT_A34".equalsIgnoreCase(msgType)) {
            ADT_A34 adtMsg = (ADT_A34) hapiMsg;


        }
    }

    /**
     *
     * @param msh
     * @return
     * @throws Exception
     */
    public static MessageHeader transformPIDToMsgHeader(MSH msh) throws Exception {
        MessageHeader messageHeader = new MessageHeader();

        HD sendingApplication = msh.getSendingApplication();
        HD sendingFacility = msh.getSendingFacility();
        HD receivingApplication = msh.getReceivingApplication();
        HD receivingFacility = msh.getReceivingFacility();
        TS dateTime = msh.getDateTimeOfMessage();
        CM_MSG msgType = msh.getMessageType();
        ST msgCtrlId = msh.getMessageControlID();
        ID versionId = msh.getVersionID();

        String msgTrigger = msh.getMessageType().getTriggerEvent().getValue();
        System.out.println(msgType + " " + msgTrigger);

        messageHeader.getSource().setSoftware(String.valueOf(sendingApplication));
        messageHeader.getSource().setName(String.valueOf(sendingFacility));
        //messageHeader.getSource().addExtension().setValue((IBaseDatatype) receivingApplication);
        messageHeader.addDestination().setName(String.valueOf(receivingFacility));

        //Date dt = dateTime;
        //messageHeader.setTimestamp(dateTime);
        messageHeader.getEvent().setCode(String.valueOf(msgType));
        //messageHeader.addExtension().setValue((IBaseDatatype) msgCtrlId);
        messageHeader.getEvent().setVersion(String.valueOf(versionId));

        return messageHeader;
    }

}