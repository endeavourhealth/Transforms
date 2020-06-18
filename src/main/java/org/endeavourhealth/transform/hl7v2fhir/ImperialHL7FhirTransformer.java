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
import org.apache.commons.io.IOUtils;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.database.dal.audit.models.Exchange;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
        String HL7Message = null;
        /*FileInputStream iS = new FileInputStream("C:\\Users\\USER\\Desktop\\Examples\\A01");
        HL7Message = IOUtils.toString(iS);*/
        //get HL7 message from the table based on id
        /*Connection connection = ConnectionManager.getHL7v2InboundConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT * from imperial where id=?";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, 1);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if(resultSet.next()) {
                        HL7Message = resultSet.getString("hl7_message");
                    }
                }
            }
            connection.commit();
        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }*/
        //get HL7 message from the table based on id

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
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
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
            CX[] patientIdList = adtMsg.getPID().getPatientIdentifierList();
            String patientGuid = String.valueOf(patientIdList[0].getIDNumber());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = transformPIDToPatient(adtMsg.getPID(), fhirPatient);

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

            TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTime());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, startDt.substring(0,8));
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
            if(fhirEpisodeOfCare == null) {
                fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setId(sourceEpisodeId);
                newEpisodeOfCare = true;
            }
            fhirEpisodeOfCare = transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
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
            String encounterId = null;
            XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
            if(consultingDoctor != null && consultingDoctor.length > 0) {
                ST idNumCd = consultingDoctor[0].getIDNumber();
                encounterId = String.valueOf(idNumCd);
            }

            String locallyUniqueResourceEncounterId = ImperialHL7Helper.createUniqueId(patientGuid, encounterId);
            fhirEncounter = (Encounter) imperialHL7Helper.retrieveResource(locallyUniqueResourceEncounterId, ResourceType.Encounter);
            if (fhirEncounter == null) {
                fhirEncounter = new Encounter();
                fhirEncounter.setId(encounterId);
                newEncounter = true;
            }
            fhirEncounter = transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter);
            if (newPatient) {
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
            }
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
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
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
            CX[] patientIdList = adtMsg.getPID().getPatientIdentifierList();
            String patientGuid = String.valueOf(patientIdList[0].getIDNumber());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = transformPIDToPatient(adtMsg.getPID(), fhirPatient);

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

            TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTime());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, startDt.substring(0,8));
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
            if(fhirEpisodeOfCare == null) {
                fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setId(sourceEpisodeId);
                newEpisodeOfCare = true;
            }
            fhirEpisodeOfCare = transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
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
            String encounterId = null;
            XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
            if(consultingDoctor != null && consultingDoctor.length > 0) {
                ST idNumCd = consultingDoctor[0].getIDNumber();
                encounterId = String.valueOf(idNumCd);
            }

            String locallyUniqueResourceEncounterId = ImperialHL7Helper.createUniqueId(patientGuid, encounterId);
            fhirEncounter = (Encounter) imperialHL7Helper.retrieveResource(locallyUniqueResourceEncounterId, ResourceType.Encounter);
            if (fhirEncounter == null) {
                fhirEncounter = new Encounter();
                fhirEncounter.setId(encounterId);
                newEncounter = true;
            }
            fhirEncounter = transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter);
            if (newPatient) {
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
            }
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
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
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
            CX[] patientIdList = adtMsg.getPID().getPatientIdentifierList();
            String patientGuid = String.valueOf(patientIdList[0].getIDNumber());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = transformPIDToPatient(adtMsg.getPID(), fhirPatient);

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

            TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTime());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, startDt.substring(0,8));
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
            if(fhirEpisodeOfCare == null) {
                fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setId(sourceEpisodeId);
                newEpisodeOfCare = true;
            }
            fhirEpisodeOfCare = transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
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
            String encounterId = null;
            XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
            if(consultingDoctor != null && consultingDoctor.length > 0) {
                ST idNumCd = consultingDoctor[0].getIDNumber();
                encounterId = String.valueOf(idNumCd);
            }

            String locallyUniqueResourceEncounterId = ImperialHL7Helper.createUniqueId(patientGuid, encounterId);
            fhirEncounter = (Encounter) imperialHL7Helper.retrieveResource(locallyUniqueResourceEncounterId, ResourceType.Encounter);
            if (fhirEncounter == null) {
                fhirEncounter = new Encounter();
                fhirEncounter.setId(encounterId);
                newEncounter = true;
            }
            fhirEncounter = transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter);
            if (newPatient) {
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
            }
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
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
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
            CX[] patientIdList = adtMsg.getPID().getPatientIdentifierList();
            String patientGuid = String.valueOf(patientIdList[0].getIDNumber());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = transformPIDToPatient(adtMsg.getPID(), fhirPatient);

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

            TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTime());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, startDt.substring(0,8));
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
            if(fhirEpisodeOfCare == null) {
                fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setId(sourceEpisodeId);
                newEpisodeOfCare = true;
            }
            fhirEpisodeOfCare = transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
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
            String encounterId = null;
            XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
            if(consultingDoctor != null && consultingDoctor.length > 0) {
                ST idNumCd = consultingDoctor[0].getIDNumber();
                encounterId = String.valueOf(idNumCd);
            }

            String locallyUniqueResourceEncounterId = ImperialHL7Helper.createUniqueId(patientGuid, encounterId);
            fhirEncounter = (Encounter) imperialHL7Helper.retrieveResource(locallyUniqueResourceEncounterId, ResourceType.Encounter);
            if (fhirEncounter == null) {
                fhirEncounter = new Encounter();
                fhirEncounter.setId(encounterId);
                newEncounter = true;
            }
            fhirEncounter = transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter);
            if (newPatient) {
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
            }
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
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            //Practitioner

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = transformPV1ToOrgLocation(adtMsg.getPV1(), fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //LocationPatientAssLoc
            Location fhirLocationPatientAssLoc = null;
            fhirLocationPatientAssLoc = new Location();
            fhirLocationPatientAssLoc = transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), fhirLocationPatientAssLoc);
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
            CX[] patientIdList = adtMsg.getPID().getPatientIdentifierList();
            String patientGuid = String.valueOf(patientIdList[0].getIDNumber());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = transformPIDToPatient(adtMsg.getPID(), fhirPatient);

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

            TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTime());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, startDt.substring(0,8));
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
            if(fhirEpisodeOfCare == null) {
                fhirEpisodeOfCare = new EpisodeOfCare();
                fhirEpisodeOfCare.setId(sourceEpisodeId);
                newEpisodeOfCare = true;
            }
            fhirEpisodeOfCare = transformPV1ToEpisodeOfCare(adtMsg.getPV1(), fhirEpisodeOfCare);
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
            String encounterId = null;
            XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
            if(consultingDoctor != null && consultingDoctor.length > 0) {
                ST idNumCd = consultingDoctor[0].getIDNumber();
                encounterId = String.valueOf(idNumCd);
            }

            String locallyUniqueResourceEncounterId = ImperialHL7Helper.createUniqueId(patientGuid, encounterId);
            fhirEncounter = (Encounter) imperialHL7Helper.retrieveResource(locallyUniqueResourceEncounterId, ResourceType.Encounter);
            if (fhirEncounter == null) {
                fhirEncounter = new Encounter();
                fhirEncounter.setId(encounterId);
                newEncounter = true;
            }
            fhirEncounter = transformPV1ToEncounter(adtMsg.getPV1(), fhirEncounter);
            if (newPatient) {
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
            }
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
        address.setPostalCode(String.valueOf(postCode).replaceAll("\\s",""));
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
            practitioner.setId(String.valueOf(idNumCd));
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
        episodeOfCare.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_EPISODE_OF_CARE));
        episodeOfCare.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);

        TS admitDtTime = pv1.getAdmitDateTime();
        String startDt = String.valueOf(admitDtTime.getTime());
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Date stDt = formatter.parse(startDt.substring(0,4)+"-"+startDt.substring(4,6)+"-"+startDt.substring(6,8));

        TS[] dischargeDtTime = pv1.getDischargeDateTime();
        Date dsDt = null;
        if(dischargeDtTime.length > 0) {
            String endDt = String.valueOf(dischargeDtTime[0].getTime());
            dsDt = formatter.parse(endDt.substring(0,4)+"-"+endDt.substring(4,6)+"-"+endDt.substring(6,8));
        }

        episodeOfCare.getPeriod().setStart(stDt);
        episodeOfCare.getPeriod().setStart(dsDt);
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
        location.setId("Imperial College Healthcare NHS Trust");
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
        location.setId(String.valueOf(pv1.getAssignedPatientLocation().getLocationDescription()));
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