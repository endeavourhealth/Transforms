package org.endeavourhealth.transform.hl7v2fhir;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v251.datatype.*;
import ca.uhn.hl7v2.model.v251.message.*;
import ca.uhn.hl7v2.model.v251.segment.MSH;
import ca.uhn.hl7v2.model.v251.segment.PID;
import ca.uhn.hl7v2.model.v251.segment.PV1;
import ca.uhn.hl7v2.parser.EncodingNotSupportedException;
import ca.uhn.hl7v2.parser.Parser;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public abstract class ImperialHL7FhirTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ImperialHL7FhirTransformer.class);

    /**
     *
     * @param exchangeBody
     * @param fhirResourceFiler
     * @param version
     * @throws Exception
     */
    public static void transform(String exchangeBody, FhirResourceFiler fhirResourceFiler, String version) throws Exception {
        //TODO - get HL7 message from the table based on id of exchange body
        String HL7Message = exchangeBody;
        //TODO - get HL7 message from the table based on id of exchange body

        Message hapiMsg = parseHL7Message(HL7Message);
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
            fhirOrganization = transformPV1ToOrganization(adtMsg.getPV1(), fhirOrganization);
            //Organization

            //Practitioner
            Practitioner fhirPractitioner = null;
            fhirPractitioner = new Practitioner();
            fhirPractitioner = transformPV1ToPractitioner(adtMsg.getPV1(), fhirPractitioner);
            fhirPractitioner.addPractitionerRole().setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(fhirPractitioner);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
            fhirLocationPatientAssLoc.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderPatientAssLoc = new LocationBuilder(fhirLocationPatientAssLoc);
            //LocationPatientAssLoc

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            //Organization

            //Patient
            CX[] patientIdList = adtMsg.getPID().getPatientIdentifierList();
            String patientGuid = String.valueOf(patientIdList[0].getIDNumber());
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
            }
            fhirPatient = transformPIDToPatient(adtMsg.getPID(), fhirPatient);
            fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            //Patient

            //EpisodeOfCare
            EpisodeOfCare fhirEpisodeOfCare = null;
            fhirEpisodeOfCare = new EpisodeOfCare();
            fhirEpisodeOfCare = transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
            fhirEpisodeOfCare.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
            fhirEpisodeOfCare.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            fhirEpisodeOfCare.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            //EpisodeOfCare

            //Encounter
            String locallyUniqueResourceEncounterId = String.valueOf(adtMsg.getPV1().getAlternateVisitID());
            Encounter fhirEncounter = null;
            Encounter existingEncounter = (Encounter) imperialHL7Helper.retrieveResource(locallyUniqueResourceEncounterId, ResourceType.Encounter);
            if (existingEncounter == null) {
                fhirEncounter = new Encounter();
            }
            fhirEncounter = transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter);
            fhirEncounter.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
            fhirEncounter.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirEncounter.addEpisodeOfCare(ImperialHL7Helper.createReference(ResourceType.EpisodeOfCare, fhirEpisodeOfCare.getId()));
            fhirEncounter.addParticipant().setIndividual(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirEncounter.addLocation().setLocation(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationPatientAssLoc.getId()));
            fhirEncounter.setServiceProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            EncounterBuilder encounterBuilder = new EncounterBuilder(fhirEncounter);
            fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
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
            fhirOrganization = transformPV1ToOrganization(adtMsg.getPV1(), fhirOrganization);
            //Organization

            //Practitioner
            Practitioner fhirPractitioner = null;
            fhirPractitioner = new Practitioner();
            fhirPractitioner = transformPV1ToPractitioner(adtMsg.getPV1(), fhirPractitioner);
            fhirPractitioner.addPractitionerRole().setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(fhirPractitioner);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
            fhirLocationPatientAssLoc.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderPatientAssLoc = new LocationBuilder(fhirLocationPatientAssLoc);
            //LocationPatientAssLoc

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            //Organization

            //Patient
            CX[] patientIdList = adtMsg.getPID().getPatientIdentifierList();
            String patientGuid = String.valueOf(patientIdList[0].getIDNumber());
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
            }
            fhirPatient = transformPIDToPatient(adtMsg.getPID(), fhirPatient);
            fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            //Patient

            //EpisodeOfCare
            EpisodeOfCare fhirEpisodeOfCare = null;
            fhirEpisodeOfCare = new EpisodeOfCare();
            fhirEpisodeOfCare = transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
            fhirEpisodeOfCare.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
            fhirEpisodeOfCare.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            fhirEpisodeOfCare.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            //EpisodeOfCare

            //Encounter
            String locallyUniqueResourceEncounterId = String.valueOf(adtMsg.getPV1().getAlternateVisitID());
            Encounter fhirEncounter = null;
            Encounter existingEncounter = (Encounter) imperialHL7Helper.retrieveResource(locallyUniqueResourceEncounterId, ResourceType.Encounter);
            if (existingEncounter == null) {
                fhirEncounter = new Encounter();
            }
            fhirEncounter = transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter);
            fhirEncounter.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
            fhirEncounter.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirEncounter.addEpisodeOfCare(ImperialHL7Helper.createReference(ResourceType.EpisodeOfCare, fhirEpisodeOfCare.getId()));
            fhirEncounter.addParticipant().setIndividual(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirEncounter.addLocation().setLocation(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));
            fhirEncounter.setServiceProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            EncounterBuilder encounterBuilder = new EncounterBuilder(fhirEncounter);
            fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
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
            fhirOrganization = transformPV1ToOrganization(adtMsg.getPV1(), fhirOrganization);
            //Organization

            //Practitioner
            Practitioner fhirPractitioner = null;
            fhirPractitioner = new Practitioner();
            fhirPractitioner = transformPV1ToPractitioner(adtMsg.getPV1(), fhirPractitioner);
            fhirPractitioner.addPractitionerRole().setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(fhirPractitioner);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
            fhirLocationPatientAssLoc.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderPatientAssLoc = new LocationBuilder(fhirLocationPatientAssLoc);
            //LocationPatientAssLoc

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            //Organization

            //Patient
            CX[] patientIdList = adtMsg.getPID().getPatientIdentifierList();
            String patientGuid = String.valueOf(patientIdList[0].getIDNumber());
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
            }
            fhirPatient = transformPIDToPatient(adtMsg.getPID(), fhirPatient);
            fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            //Patient

            //EpisodeOfCare
            EpisodeOfCare fhirEpisodeOfCare = null;
            fhirEpisodeOfCare = new EpisodeOfCare();
            fhirEpisodeOfCare = transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
            fhirEpisodeOfCare.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
            fhirEpisodeOfCare.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            fhirEpisodeOfCare.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            //EpisodeOfCare

            //Encounter
            String locallyUniqueResourceEncounterId = String.valueOf(adtMsg.getPV1().getAlternateVisitID());
            Encounter fhirEncounter = null;
            Encounter existingEncounter = (Encounter) imperialHL7Helper.retrieveResource(locallyUniqueResourceEncounterId, ResourceType.Encounter);
            if (existingEncounter == null) {
                fhirEncounter = new Encounter();
            }
            fhirEncounter = transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter);
            fhirEncounter.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
            fhirEncounter.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirEncounter.addEpisodeOfCare(ImperialHL7Helper.createReference(ResourceType.EpisodeOfCare, fhirEpisodeOfCare.getId()));
            fhirEncounter.addParticipant().setIndividual(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirEncounter.addLocation().setLocation(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));
            fhirEncounter.setServiceProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            EncounterBuilder encounterBuilder = new EncounterBuilder(fhirEncounter);
            fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
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
            fhirOrganization = transformPV1ToOrganization(adtMsg.getPV1(), fhirOrganization);
            //Organization

            //Practitioner
            Practitioner fhirPractitioner = null;
            fhirPractitioner = new Practitioner();
            fhirPractitioner = transformPV1ToPractitioner(adtMsg.getPV1(), fhirPractitioner);
            fhirPractitioner.addPractitionerRole().setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(fhirPractitioner);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
            fhirLocationPatientAssLoc.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderPatientAssLoc = new LocationBuilder(fhirLocationPatientAssLoc);
            //LocationPatientAssLoc

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            //Organization

            //Patient
            CX[] patientIdList = adtMsg.getPID().getPatientIdentifierList();
            String patientGuid = String.valueOf(patientIdList[0].getIDNumber());
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
            }
            fhirPatient = transformPIDToPatient(adtMsg.getPID(), fhirPatient);
            fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            //Patient

            //EpisodeOfCare
            EpisodeOfCare fhirEpisodeOfCare = null;
            fhirEpisodeOfCare = new EpisodeOfCare();
            fhirEpisodeOfCare = transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
            fhirEpisodeOfCare.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
            fhirEpisodeOfCare.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            fhirEpisodeOfCare.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            //EpisodeOfCare

            //Encounter
            String locallyUniqueResourceEncounterId = String.valueOf(adtMsg.getPV1().getAlternateVisitID());
            Encounter fhirEncounter = null;
            Encounter existingEncounter = (Encounter) imperialHL7Helper.retrieveResource(locallyUniqueResourceEncounterId, ResourceType.Encounter);
            if (existingEncounter == null) {
                fhirEncounter = new Encounter();
            }
            fhirEncounter = transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter);
            fhirEncounter.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
            fhirEncounter.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirEncounter.addEpisodeOfCare(ImperialHL7Helper.createReference(ResourceType.EpisodeOfCare, fhirEpisodeOfCare.getId()));
            fhirEncounter.addParticipant().setIndividual(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirEncounter.addLocation().setLocation(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));
            fhirEncounter.setServiceProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            EncounterBuilder encounterBuilder = new EncounterBuilder(fhirEncounter);
            fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
            //Encounter

        } else {
            ADT_AXX adtMsg = (ADT_AXX) hapiMsg;

            //MessageHeader
            MessageHeader fhirMessageHeader = null;
            fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
            //MessageHeader

            //Organization
            Organization fhirOrganization = null;
            fhirOrganization = new Organization();
            fhirOrganization = transformPV1ToOrganization(adtMsg.getPV1(), fhirOrganization);
            //Organization

            //Practitioner
            Practitioner fhirPractitioner = null;
            fhirPractitioner = new Practitioner();
            fhirPractitioner = transformPV1ToPractitioner(adtMsg.getPV1(), fhirPractitioner);
            fhirPractitioner.addPractitionerRole().setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(fhirPractitioner);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
            fhirLocationPatientAssLoc.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderPatientAssLoc = new LocationBuilder(fhirLocationPatientAssLoc);
            //LocationPatientAssLoc

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            //Organization

            //Patient
            CX[] patientIdList = adtMsg.getPID().getPatientIdentifierList();
            String patientGuid = String.valueOf(patientIdList[0].getIDNumber());
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
            }
            fhirPatient = transformPIDToPatient(adtMsg.getPID(), fhirPatient);
            fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            //Patient

            //EpisodeOfCare
            EpisodeOfCare fhirEpisodeOfCare = null;
            fhirEpisodeOfCare = new EpisodeOfCare();
            fhirEpisodeOfCare = transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
            fhirEpisodeOfCare.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
            fhirEpisodeOfCare.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
            fhirEpisodeOfCare.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));

            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(fhirEpisodeOfCare);
            //EpisodeOfCare

            //Encounter
            String locallyUniqueResourceEncounterId = String.valueOf(adtMsg.getPV1().getAlternateVisitID());
            Encounter fhirEncounter = null;
            Encounter existingEncounter = (Encounter) imperialHL7Helper.retrieveResource(locallyUniqueResourceEncounterId, ResourceType.Encounter);
            if (existingEncounter == null) {
                fhirEncounter = new Encounter();
            }
            fhirEncounter = transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter);
            fhirEncounter.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, fhirPatient.getId()));
            fhirEncounter.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirEncounter.addEpisodeOfCare(ImperialHL7Helper.createReference(ResourceType.EpisodeOfCare, fhirEpisodeOfCare.getId()));
            fhirEncounter.addParticipant().setIndividual(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));
            fhirEncounter.addLocation().setLocation(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));
            fhirEncounter.setServiceProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            EncounterBuilder encounterBuilder = new EncounterBuilder(fhirEncounter);
            fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
            //Encounter

        }
    }

    /**
     *
     * @param hl7Message
     * @return
     * @throws Exception
     */
    private static Message parseHL7Message(String hl7Message) throws Exception {
        HapiContext context = new DefaultHapiContext();
        context.getParserConfiguration().setValidating(false);
        context.getParserConfiguration().setAllowUnknownVersions(true);
        Parser p = context.getGenericParser();
        Message hapiMsg = null;
        try {
            // The parse method performs the actual parsing
            hapiMsg = p.parse(hl7Message);
        } catch (EncodingNotSupportedException e) {
            e.printStackTrace();
        } catch (HL7Exception e) {
            e.printStackTrace();
        }
        return hapiMsg;
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
        MSG msgType = msh.getMessageType();
        ST msgCtrlId = msh.getMessageControlID();
        VID versionId = msh.getVersionID();

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

    /**
     *
     * @param pid
     * @param patient
     * @return
     * @throws Exception
     */
    public static Patient transformPIDToPatient(PID pid, Patient patient) throws Exception {
        patient.setId(UUID.randomUUID().toString());
        patient.getMeta().addProfile("http://endeavourhealth.org/fhir/StructureDefinition/primarycare-patient");

        CX[] patientIdList = pid.getPatientIdentifierList();
        ST id = patientIdList[0].getIDNumber();
        ST nhsNumber = patientIdList[1].getIDNumber();

        patient.addIdentifier().setValue(String.valueOf(id)).setSystem("http://imperial-uk.com/identifier/patient-id").setUse(Identifier.IdentifierUse.fromCode("secondary"));
        patient.addIdentifier().setSystem("http://fhir.nhs.net/Id/nhs-number")
                .setValue(String.valueOf(nhsNumber)).setUse(Identifier.IdentifierUse.fromCode("official"));

        XPN[] patientName = pid.getPatientName();
        FN familyName = patientName[0].getFamilyName();
        ST givenName = patientName[0].getGivenName();
        ID nameTypeCode = patientName[0].getNameTypeCode();
        ST prefix = patientName[0].getPrefixEgDR();
        patient.addName().addFamily(String.valueOf(familyName)).addPrefix(String.valueOf(prefix)).addGiven(String.valueOf(givenName)).setUse(HumanName.NameUse.OFFICIAL);

        IS gender = pid.getAdministrativeSex();
        switch(String.valueOf(gender)) {
            case "M":
                patient.setGender(Enumerations.AdministrativeGender.MALE);
                break;
            case "F":
                patient.setGender(Enumerations.AdministrativeGender.FEMALE);
                break;
            default:
                // code block
        }

        TS dob = pid.getDateTimeOfBirth();
        if (!dob.isEmpty()) {
            String dtB = String.valueOf(dob.getTime());
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            Date date = formatter.parse(dtB.substring(0,4)+"-"+dtB.substring(4,6)+"-"+dtB.substring(6,8));
            patient.setBirthDate(date);
        }

        XAD[] patientAddress = pid.getPatientAddress();
        ID addType = patientAddress[0].getAddressType();
        ST city = patientAddress[0].getCity();
        ID country = patientAddress[0].getCountry();
        SAD add = patientAddress[0].getStreetAddress();
        ST postCode = patientAddress[0].getZipOrPostalCode();

        Address address = new Address();
        if (String.valueOf(addType).equals("HOME")) {address.setUse(Address.AddressUse.HOME);}
        if (String.valueOf(addType).equals("TEMP")) {address.setUse(Address.AddressUse.TEMP);}
        if (String.valueOf(addType).equals("OLD")) {address.setUse(Address.AddressUse.OLD);}

        address.addLine(String.valueOf(add));
        address.setCountry(String.valueOf(country));
        address.setPostalCode(String.valueOf(postCode));
        address.setCity(String.valueOf(city));
        address.setDistrict("");
        patient.addAddress(address);

        TS dod = pid.getPatientDeathDateAndTime();
        if (!dod.isEmpty()) {
            BooleanType bool = new BooleanType();
            bool.setValue(true);
            patient.setDeceased(bool);
        }
        return patient;
    }

    /**
     *
     * @param pv1
     * @param encounter
     * @return
     * @throws Exception
     */
    public static Encounter transformPV1ToEncounter(PV1 pv1, Encounter encounter) throws Exception {
        PL assignedPatientLoc = pv1.getAssignedPatientLocation();
        IS accStatus = pv1.getAccountStatus();
        TS admitDtTime = pv1.getAdmitDateTime();
        String startDt = String.valueOf(admitDtTime.getTime());
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Date stDt = formatter.parse(startDt.substring(0,4)+"-"+startDt.substring(4,6)+"-"+startDt.substring(6,8));

        /*TS[] dischargeDtTime = pv1.getDischargeDateTime();
        String endDt = String.valueOf(dischargeDtTime[0].getTime());
        Date dsDt = formatter.parse(endDt.substring(0,4)+"-"+endDt.substring(4,6)+"-"+endDt.substring(6,8));*/

        IS patientType = pv1.getPatientType();
        IS servicingFacility = pv1.getServicingFacility();

        encounter.setId(UUID.randomUUID().toString());
        encounter.getMeta().addProfile("http://endeavourhealth.org/fhir/StructureDefinition/primarycare-encounter");

        /*if (String.valueOf(accStatus).equalsIgnoreCase("active")) {*/
            encounter.setStatus(Encounter.EncounterState.INPROGRESS);
        /*} else {
            encounter.setStatus(Encounter.EncounterState.FINISHED);
        }*/
        encounter.getPeriod().setStart(stDt);
        /*encounter.getPeriod().setEnd(dsDt);*/
        return encounter;
    }

    /**
     *
     * @param pv1
     * @param practitioner
     * @return
     * @throws Exception
     */
    public static Practitioner transformPV1ToPractitioner(PV1 pv1, Practitioner practitioner) throws Exception {
        practitioner.setId(UUID.randomUUID().toString());
        practitioner.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PRACTITIONER));
        practitioner.setActive(true);

        /*XCN[] referringDoctor = pv1.getReferringDoctor();
        if(referringDoctor != null && referringDoctor.length > 0) {
            ST idNumRd = referringDoctor[0].getIDNumber();
            FN familyNameRd = referringDoctor[0].getFamilyName();
            ST givenNameRd = referringDoctor[0].getGivenName();
            HD assigningAuthorityRd = referringDoctor[0].getAssigningAuthority();

            Identifier identifierRd = new Identifier();
            identifierRd.setValue(String.valueOf(idNumRd));
            identifierRd.setSystem(String.valueOf(assigningAuthorityRd.getNamespaceID()));
            practitioner.addIdentifier(identifierRd);

            HumanName name = practitioner.getName();
            name.setUse(HumanName.NameUse.USUAL);
            name.setText(String.valueOf(givenNameRd));
            practitioner.setName(name);
        }

        XCN[] attendingDoctor = pv1.getAttendingDoctor();
        if(attendingDoctor != null && attendingDoctor.length > 0) {
            ST idNumAd = attendingDoctor[0].getIDNumber();
            FN familyNameAd = attendingDoctor[0].getFamilyName();
            ST givenNameAd = attendingDoctor[0].getGivenName();
            HD assigningAuthorityAd = attendingDoctor[0].getAssigningAuthority();

            Identifier identifierAd = new Identifier();
            identifierAd.setValue(String.valueOf(idNumAd));
            identifierAd.setSystem(String.valueOf(assigningAuthorityAd.getNamespaceID()));
            practitioner.addIdentifier(identifierAd);
        }*/

        XCN[] consultingDoctor = pv1.getConsultingDoctor();
        if(consultingDoctor != null && consultingDoctor.length > 0) {
            ST idNumCd = consultingDoctor[0].getIDNumber();
            FN familyNameCd = consultingDoctor[0].getFamilyName();
            ST givenNameCd = consultingDoctor[0].getGivenName();
            HD assigningAuthorityCd = consultingDoctor[0].getAssigningAuthority();

            Identifier identifierCd = new Identifier();
            identifierCd.setValue(String.valueOf(idNumCd));
            identifierCd.setSystem("http://endeavourhealth.org/fhir/Identifier/gmc-number");
            practitioner.addIdentifier(identifierCd);
        }

        return practitioner;
    }

    /**
     *
     * @param pv1
     * @param episodeOfCare
     * @return
     * @throws Exception
     */
    public static EpisodeOfCare transformPV1ToEpisodeOfCare(PV1 pv1, EpisodeOfCare episodeOfCare) throws Exception {
        episodeOfCare.setId(UUID.randomUUID().toString());
        episodeOfCare.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_EPISODE_OF_CARE));
        episodeOfCare.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);

        TS admitDtTime = pv1.getAdmitDateTime();
        String startDt = String.valueOf(admitDtTime.getTime());
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Date stDt = formatter.parse(startDt.substring(0,4)+"-"+startDt.substring(4,6)+"-"+startDt.substring(6,8));

        TS[] dischargeDtTime = pv1.getDischargeDateTime();
        String endDt = String.valueOf(dischargeDtTime[0].getTime());
        Date dsDt = formatter.parse(endDt.substring(0,4)+"-"+endDt.substring(4,6)+"-"+endDt.substring(6,8));

        Period period = episodeOfCare.getPeriod();
        if (period == null) {
            period = new Period();
            period.setStart(stDt);
            period.setEnd(dsDt);
            episodeOfCare.setPeriod(period);
        }

        return episodeOfCare;
    }

    /**
     *
     * @param pv1
     * @param location
     * @return
     * @throws Exception
     */
    public static Location transformPV1ToOrgLocation(PV1 pv1, Location location) throws Exception {
        location.setId(UUID.randomUUID().toString());
        location.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_LOCATION));
        location.setStatus(Location.LocationStatus.ACTIVE);
        location.setName("Imperial College Healthcare NHS Trust");
        location.setDescription("Imperial College Healthcare NHS Trust");
        location.setMode(Location.LocationMode.INSTANCE);

        Address address = new Address();
        address.setUse(Address.AddressUse.WORK);
        location.setAddress(address);

        return location;
    }

    /**
     *
     * @param pv1
     * @param location
     * @return
     * @throws Exception
     */
    public static Location transformPV1ToPatientAssignedLocation(PV1 pv1, Location location) throws Exception {
        location.setId(UUID.randomUUID().toString());
        location.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_LOCATION));
        location.setStatus(Location.LocationStatus.ACTIVE);
        location.setName(String.valueOf(pv1.getAssignedPatientLocation().getLocationDescription()));
        location.setDescription(String.valueOf(pv1.getAssignedPatientLocation().getLocationDescription()));
        location.setMode(Location.LocationMode.INSTANCE);

        Address address = new Address();
        address.setUse(Address.AddressUse.WORK);
        address.addLine(String.valueOf(pv1.getAssignedPatientLocation().getLocationDescription()));
        address.setCity(String.valueOf(pv1.getAssignedPatientLocation().getBuilding()));
        location.setAddress(address);

        return location;
    }

    /**
     *
     * @param pv1
     * @param organization
     * @return
     * @throws Exception
     */
    public static Organization transformPV1ToOrganization(PV1 pv1, Organization organization) throws Exception {
        organization.setId(UUID.randomUUID().toString());
        organization.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_ORGANIZATION));

        Identifier identifier = new Identifier();
        identifier.setUse(Identifier.IdentifierUse.fromCode("official"));
        identifier.setSystem("http://fhir.nhs.net/Id/ods-organization-code");
        identifier.setValue("RYJ");
        organization.addIdentifier(identifier);

        PL assignedPatientLocation = pv1.getAssignedPatientLocation();

        Address address = new Address();
        address.setUse(Address.AddressUse.WORK);
        /*address.setText("a");
        address.addLine(String.valueOf(assignedPatientLocation.getLocationDescription()));
        address.setCity(String.valueOf(assignedPatientLocation.getBuilding()));
        address.setDistrict("b");
        address.setPostalCode("c");*/
        organization.addAddress(address);

        organization.setName("Imperial College Healthcare NHS Trust");
        return organization;
    }

}