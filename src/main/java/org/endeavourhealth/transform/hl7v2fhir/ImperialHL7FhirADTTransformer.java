package org.endeavourhealth.transform.hl7v2fhir;

import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v23.datatype.*;
import ca.uhn.hl7v2.model.v23.message.*;
import ca.uhn.hl7v2.model.v23.segment.MSH;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.fhirhl7v2.FhirHl7v2Filer;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.endeavourhealth.transform.hl7v2fhir.transforms.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ImperialHL7FhirADTTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ImperialHL7FhirADTTransformer.class);

    /**
     *
     * @param fhirResourceFiler
     * @param version
     * @param hapiMsg
     * @throws Exception
     */
    public static void transform(FhirResourceFiler fhirResourceFiler, String version, Message hapiMsg) throws Exception {
        String msgType = (hapiMsg.printStructure()).substring(0,7);
        ImperialHL7Helper imperialHL7Helper = new ImperialHL7Helper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(),
                fhirResourceFiler.getExchangeId(), null, null);

        if("ADT_A01".equalsIgnoreCase(msgType)) {
            transformADT_A01(fhirResourceFiler, (ADT_A01) hapiMsg, msgType, imperialHL7Helper);

        } else if("ADT_A02".equalsIgnoreCase(msgType)) {
            transformADT_A02(fhirResourceFiler, (ADT_A02) hapiMsg, msgType, imperialHL7Helper);

        } else if("ADT_A03".equalsIgnoreCase(msgType)) {
            transformADT_A03(fhirResourceFiler, (ADT_A03) hapiMsg, msgType, imperialHL7Helper);

        } else if("ADT_A11".equalsIgnoreCase(msgType)) {
            transformADT_A11(fhirResourceFiler, (ADT_A11) hapiMsg, imperialHL7Helper);

        } else if("ADT_A12".equalsIgnoreCase(msgType)) {
            transformADT_A12(fhirResourceFiler, (ADT_A12) hapiMsg, imperialHL7Helper);

        } else if("ADT_A13".equalsIgnoreCase(msgType)) {
            transformADT_A13(fhirResourceFiler, (ADT_A13) hapiMsg, imperialHL7Helper);

        } else if("ADT_A08".equalsIgnoreCase(msgType)) {
            transformADT_A08(fhirResourceFiler, (ADT_A08) hapiMsg, msgType, imperialHL7Helper);

        } else if("ADT_A28".equalsIgnoreCase(msgType)) {
            transformADT_A28(fhirResourceFiler, (ADT_A28) hapiMsg, msgType, imperialHL7Helper);

        } else if("ADT_A31".equalsIgnoreCase(msgType)) {
            transformADT_A31(fhirResourceFiler, (ADT_A31) hapiMsg, msgType, imperialHL7Helper);

        } else if("ADT_A34".equalsIgnoreCase(msgType)) {
            transformADT_A34(fhirResourceFiler, (ADT_A34) hapiMsg);
        }
    }

    private static void transformADT_A34(FhirResourceFiler fhirResourceFiler, ADT_A34 hapiMsg) throws Exception {
        ADT_A34 adtMsg = hapiMsg;
        FhirHl7v2Filer.AdtResourceFiler filer = new FhirHl7v2Filer.AdtResourceFiler(fhirResourceFiler);

        PatientTransformer.performA34PatientMerge(filer, adtMsg.getPID(), adtMsg.getMRG());
    }

    private static void transformADT_A31(FhirResourceFiler fhirResourceFiler, ADT_A31 hapiMsg, String msgType, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A31 adtMsg = hapiMsg;

        //MessageHeader
        MessageHeader fhirMessageHeader = null;
        fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
        //MessageHeader

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();
        practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1().getConsultingDoctor(), practitionerBuilderConsulting);

        PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
        roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();
        practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1().getReferringDoctor(), practitionerBuilderReferring);

        PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
        roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        //Practitioner

        //LocationOrg
        LocationBuilder locationBuilderOrg = null;
        locationBuilderOrg = new LocationBuilder();
        locationBuilderOrg = LocationTransformer.transformPV1ToOrgLocation(locationBuilderOrg);
        locationBuilderOrg.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
        //LocationOrg

        //LocationPatientAssLoc
        LocationBuilder locationBuilderPatientAssLoc = null;
        locationBuilderPatientAssLoc = new LocationBuilder();
        locationBuilderPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), locationBuilderPatientAssLoc);
        locationBuilderPatientAssLoc.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
        //LocationPatientAssLoc

        //Organization
        organizationBuilder.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, organizationBuilder);
        //Organization

        //Patient
        PatientBuilder patientBuilder = null;
        CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());
        boolean newPatient = false;
        Patient existingPatient = null;
        existingPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
        if (existingPatient != null) {
            patientBuilder = new PatientBuilder(existingPatient);
        } else {
            patientBuilder = new PatientBuilder();
            newPatient = true;
        }

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper);

        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderConsulting.getResourceId()));

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);
        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);
            patientBuilder.addCareProvider(organisationReference);

            Reference practitionerReference = imperialHL7Helper.createPractitionerReference(practitionerBuilderConsulting.getResourceId());
            practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerReference);

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
        fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitNum, ResourceType.EpisodeOfCare);
        EpisodeOfCareBuilder episodeOfCareBuilder =null;
        if(fhirEpisodeOfCare == null) {
            // fhirEpisodeOfCare = new EpisodeOfCare();
            episodeOfCareBuilder=new EpisodeOfCareBuilder();
            episodeOfCareBuilder.setId(visitNum);
            newEpisodeOfCare = true;
        }

        else if(fhirEpisodeOfCare != null) {
            episodeOfCareBuilder=new EpisodeOfCareBuilder(fhirEpisodeOfCare);
        }


        episodeOfCareBuilder = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), episodeOfCareBuilder);
        if (newEpisodeOfCare) {
            episodeOfCareBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientGuid));
            episodeOfCareBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            episodeOfCareBuilder.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderConsulting.getResourceId()));
        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientGuid);
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
            episodeOfCareBuilder.setManagingOrganisation(patientReference);

            Reference organizationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
            episodeOfCareBuilder.setManagingOrganisation(organizationReference);

            Reference practitionerReference = imperialHL7Helper.createPractitionerReference(practitionerBuilderConsulting.getResourceId());
            practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            episodeOfCareBuilder.setCareManager(practitionerReference);
        }


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

        //Encounter
    }

    private static void transformADT_A28(FhirResourceFiler fhirResourceFiler, ADT_A28 hapiMsg, String msgType, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A28 adtMsg = hapiMsg;

        //MessageHeader
        MessageHeader fhirMessageHeader = null;
        fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
        //MessageHeader

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();
        practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1().getConsultingDoctor(), practitionerBuilderConsulting);

        PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
        roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();
        practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1().getReferringDoctor(), practitionerBuilderReferring);

        PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
        roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        //Practitioner

        //LocationOrg
        LocationBuilder locationBuilderOrg = null;
        locationBuilderOrg = new LocationBuilder();
        locationBuilderOrg = LocationTransformer.transformPV1ToOrgLocation(locationBuilderOrg);
        locationBuilderOrg.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
        //LocationOrg

        //LocationPatientAssLoc
        LocationBuilder locationBuilderPatientAssLoc = null;
        locationBuilderPatientAssLoc = new LocationBuilder();
        locationBuilderPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), locationBuilderPatientAssLoc);
        locationBuilderPatientAssLoc.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
        //LocationPatientAssLoc

        //Organization
        organizationBuilder.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, organizationBuilder);
        //Organization

        //Patient
        PatientBuilder patientBuilder = null;
        CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());
        boolean newPatient = false;
        Patient existingPatient = null;
        existingPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
        if (existingPatient != null) {
            patientBuilder = new PatientBuilder(existingPatient);
        } else {
            patientBuilder = new PatientBuilder();
            newPatient = true;
        }

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper);

        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderConsulting.getResourceId()));

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);
        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);
            patientBuilder.addCareProvider(organisationReference);

            Reference practitionerReference = imperialHL7Helper.createPractitionerReference(practitionerBuilderConsulting.getResourceId());
            practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerReference);

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
        //Patient

        boolean newEpisodeOfCare = false;
        EpisodeOfCare fhirEpisodeOfCare = null;

            /*TS admitDtTime = adtMsg.getPV1().getAdmitDateTime();
            String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());*/
        String visitNum = String.valueOf(adtMsg.getPV1().getVisitNumber().getID());
        String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
        fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitNum, ResourceType.EpisodeOfCare);
        EpisodeOfCareBuilder episodeOfCareBuilder =null;
        if(fhirEpisodeOfCare == null) {
            // fhirEpisodeOfCare = new EpisodeOfCare();
            episodeOfCareBuilder=new EpisodeOfCareBuilder();
            episodeOfCareBuilder.setId(visitNum);
            newEpisodeOfCare = true;
        }

        else if(fhirEpisodeOfCare != null) {
            episodeOfCareBuilder=new EpisodeOfCareBuilder(fhirEpisodeOfCare);
        }


        episodeOfCareBuilder = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), episodeOfCareBuilder);
        if (newEpisodeOfCare) {
            episodeOfCareBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientGuid));
            episodeOfCareBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            episodeOfCareBuilder.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderConsulting.getResourceId()));
        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientGuid);
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
            episodeOfCareBuilder.setManagingOrganisation(patientReference);

            Reference organizationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
            episodeOfCareBuilder.setManagingOrganisation(organizationReference);

            Reference practitionerReference = imperialHL7Helper.createPractitionerReference(practitionerBuilderConsulting.getResourceId());
            practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            episodeOfCareBuilder.setCareManager(practitionerReference);
        }


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

        //Encounter
    }

    private static void transformADT_A08(FhirResourceFiler fhirResourceFiler, ADT_A08 hapiMsg, String msgType, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A08 adtMsg = hapiMsg;

        //MessageHeader
        MessageHeader fhirMessageHeader = null;
        fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
        //MessageHeader

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();
        practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1().getConsultingDoctor(), practitionerBuilderConsulting);

        PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
        roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();
        practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1().getReferringDoctor(), practitionerBuilderReferring);

        PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
        roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        //Practitioner

        //LocationOrg
        LocationBuilder locationBuilderOrg = null;
        locationBuilderOrg = new LocationBuilder();
        locationBuilderOrg = LocationTransformer.transformPV1ToOrgLocation(locationBuilderOrg);
        locationBuilderOrg.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
        //LocationOrg

        //LocationPatientAssLoc
        LocationBuilder locationBuilderPatientAssLoc = null;
        locationBuilderPatientAssLoc = new LocationBuilder();
        locationBuilderPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), locationBuilderPatientAssLoc);
        locationBuilderPatientAssLoc.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
        //LocationPatientAssLoc

        //Organization
        organizationBuilder.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, organizationBuilder);
        //Organization

        //Patient
        PatientBuilder patientBuilder = null;
        CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());
        boolean newPatient = false;
        Patient existingPatient = null;
        existingPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
        if (existingPatient != null) {
            patientBuilder = new PatientBuilder(existingPatient);
        } else {
            patientBuilder = new PatientBuilder();
            newPatient = true;
        }

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper);

        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderConsulting.getResourceId()));

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);
        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);
            patientBuilder.addCareProvider(organisationReference);

            Reference practitionerReference = imperialHL7Helper.createPractitionerReference(practitionerBuilderConsulting.getResourceId());
            practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerReference);

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
        fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitNum, ResourceType.EpisodeOfCare);
        EpisodeOfCareBuilder episodeOfCareBuilder =null;
        if(fhirEpisodeOfCare == null) {
            // fhirEpisodeOfCare = new EpisodeOfCare();
            episodeOfCareBuilder=new EpisodeOfCareBuilder();
            episodeOfCareBuilder.setId(visitNum);
            newEpisodeOfCare = true;
        }

        else if(fhirEpisodeOfCare != null) {
            episodeOfCareBuilder=new EpisodeOfCareBuilder(fhirEpisodeOfCare);
        }


        episodeOfCareBuilder = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), episodeOfCareBuilder);
        if (newEpisodeOfCare) {
            episodeOfCareBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientGuid));
            episodeOfCareBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            episodeOfCareBuilder.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderConsulting.getResourceId()));
        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientGuid);
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
            episodeOfCareBuilder.setManagingOrganisation(patientReference);

            Reference organizationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
            episodeOfCareBuilder.setManagingOrganisation(organizationReference);

            Reference practitionerReference = imperialHL7Helper.createPractitionerReference(practitionerBuilderConsulting.getResourceId());
            practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            episodeOfCareBuilder.setCareManager(practitionerReference);
        }


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

        //Encounter
    }

    private static void transformADT_A13(FhirResourceFiler fhirResourceFiler, ADT_A13 hapiMsg, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A13 adtMsg = hapiMsg;
        //Encounter
        EncounterTransformer.deleteEncounterAndChildren(adtMsg.getPV1(), fhirResourceFiler, imperialHL7Helper);
        //Encounter

        //EpisodeOfCare
        String visitNum = String.valueOf(adtMsg.getPV1().getVisitNumber().getID());
        CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());
        String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
        EpisodeOfCare episodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitNum, ResourceType.EpisodeOfCare);
        if(episodeOfCare != null) {
            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(episodeOfCare);
            fhirResourceFiler.deletePatientResource(null, false, episodeOfCareBuilder);
        }
        //EpisodeOfCare
    }

    private static void transformADT_A12(FhirResourceFiler fhirResourceFiler, ADT_A12 hapiMsg, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A12 adtMsg = hapiMsg;
        //Encounter
        EncounterTransformer.deleteEncounterAndChildren(adtMsg.getPV1(), fhirResourceFiler, imperialHL7Helper);
        //Encounter

        //EpisodeOfCare
        String visitNum = String.valueOf(adtMsg.getPV1().getVisitNumber().getID());
        CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());
        String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
        EpisodeOfCare episodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitNum, ResourceType.EpisodeOfCare);
        if(episodeOfCare != null) {
            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(episodeOfCare);
            fhirResourceFiler.deletePatientResource(null, false, episodeOfCareBuilder);
        }
        //EpisodeOfCare
    }

    private static void transformADT_A11(FhirResourceFiler fhirResourceFiler, ADT_A11 hapiMsg, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A11 adtMsg = hapiMsg;
        //Encounter
        EncounterTransformer.deleteEncounterAndChildren(adtMsg.getPV1(), fhirResourceFiler, imperialHL7Helper);
        //Encounter

        //EpisodeOfCare
        String visitNum = String.valueOf(adtMsg.getPV1().getVisitNumber().getID());
        CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());
        String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, visitNum);
        EpisodeOfCare episodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitNum, ResourceType.EpisodeOfCare);
        if(episodeOfCare != null) {
            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(episodeOfCare);
            fhirResourceFiler.deletePatientResource(null, false, episodeOfCareBuilder);
        }
        //EpisodeOfCare
    }

    private static void transformADT_A03(FhirResourceFiler fhirResourceFiler, ADT_A03 hapiMsg, String msgType, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A03 adtMsg = hapiMsg;

        //MessageHeader
        MessageHeader fhirMessageHeader = null;
        fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
        //MessageHeader

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();
        practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1().getConsultingDoctor(), practitionerBuilderConsulting);

        PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
        roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();
        practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1().getReferringDoctor(), practitionerBuilderReferring);

        PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
        roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        //Practitioner

        //LocationOrg
        LocationBuilder locationBuilderOrg = null;
        locationBuilderOrg = new LocationBuilder();
        locationBuilderOrg = LocationTransformer.transformPV1ToOrgLocation(locationBuilderOrg);
        locationBuilderOrg.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
        //LocationOrg

        //LocationPatientAssLoc
        LocationBuilder locationBuilderPatientAssLoc = null;
        locationBuilderPatientAssLoc = new LocationBuilder();
        locationBuilderPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), locationBuilderPatientAssLoc);
        locationBuilderPatientAssLoc.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
        //LocationPatientAssLoc

        //Organization
        organizationBuilder.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, organizationBuilder);
        //Organization

        //Patient
        PatientBuilder patientBuilder = null;
        CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());
        boolean newPatient = false;
        Patient existingPatient = null;
        existingPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
        if (existingPatient != null) {
            patientBuilder = new PatientBuilder(existingPatient);
        } else {
            patientBuilder = new PatientBuilder();
            newPatient = true;
        }

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper);

        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderConsulting.getResourceId()));

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);
        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);
            patientBuilder.addCareProvider(organisationReference);

            Reference practitionerReference = imperialHL7Helper.createPractitionerReference(practitionerBuilderConsulting.getResourceId());
            practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerReference);

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
        fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitNum, ResourceType.EpisodeOfCare);
        EpisodeOfCareBuilder episodeOfCareBuilder =null;
        if(fhirEpisodeOfCare == null) {
            // fhirEpisodeOfCare = new EpisodeOfCare();
            episodeOfCareBuilder=new EpisodeOfCareBuilder();
            episodeOfCareBuilder.setId(visitNum);
            newEpisodeOfCare = true;
        }

        else if(fhirEpisodeOfCare != null) {
            episodeOfCareBuilder=new EpisodeOfCareBuilder(fhirEpisodeOfCare);
        }


        episodeOfCareBuilder = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), episodeOfCareBuilder);
        if (newEpisodeOfCare) {
            episodeOfCareBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientGuid));
            episodeOfCareBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            episodeOfCareBuilder.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderConsulting.getResourceId()));
        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientGuid);
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
            episodeOfCareBuilder.setManagingOrganisation(patientReference);

            Reference organizationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
            episodeOfCareBuilder.setManagingOrganisation(organizationReference);

            Reference practitionerReference = imperialHL7Helper.createPractitionerReference(practitionerBuilderConsulting.getResourceId());
            practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            episodeOfCareBuilder.setCareManager(practitionerReference);
        }


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

        //Encounter
    }

    private static void transformADT_A02(FhirResourceFiler fhirResourceFiler, ADT_A02 hapiMsg, String msgType, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A02 adtMsg = hapiMsg;

        //MessageHeader
        MessageHeader fhirMessageHeader = null;
        fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
        //MessageHeader

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();
        practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1().getConsultingDoctor(), practitionerBuilderConsulting);

        PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
        roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();
        practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1().getReferringDoctor(), practitionerBuilderReferring);

        PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
        roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        //Practitioner

        //LocationOrg
        LocationBuilder locationBuilderOrg = null;
        locationBuilderOrg = new LocationBuilder();
        locationBuilderOrg = LocationTransformer.transformPV1ToOrgLocation(locationBuilderOrg);
        locationBuilderOrg.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
        //LocationOrg

        //LocationPatientAssLoc
        LocationBuilder locationBuilderPatientAssLoc = null;
        locationBuilderPatientAssLoc = new LocationBuilder();
        locationBuilderPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), locationBuilderPatientAssLoc);
        locationBuilderPatientAssLoc.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
        //LocationPatientAssLoc

        //Organization
        organizationBuilder.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, organizationBuilder);
        //Organization

        //Patient
        PatientBuilder patientBuilder = null;
        CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());
        boolean newPatient = false;
        Patient existingPatient = null;
        existingPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
        if (existingPatient != null) {
            patientBuilder = new PatientBuilder(existingPatient);
        } else {
            patientBuilder = new PatientBuilder();
            newPatient = true;
        }

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper);

        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderConsulting.getResourceId()));

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);
        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);
            patientBuilder.addCareProvider(organisationReference);

            Reference practitionerReference = imperialHL7Helper.createPractitionerReference(practitionerBuilderConsulting.getResourceId());
            practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerReference);

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
        fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitNum, ResourceType.EpisodeOfCare);
        EpisodeOfCareBuilder episodeOfCareBuilder =null;
        if(fhirEpisodeOfCare == null) {
            // fhirEpisodeOfCare = new EpisodeOfCare();
            episodeOfCareBuilder=new EpisodeOfCareBuilder();
            episodeOfCareBuilder.setId(visitNum);
            newEpisodeOfCare = true;
        }

        else if(fhirEpisodeOfCare != null) {
            episodeOfCareBuilder=new EpisodeOfCareBuilder(fhirEpisodeOfCare);
        }


        episodeOfCareBuilder = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), episodeOfCareBuilder);
        if (newEpisodeOfCare) {
            episodeOfCareBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientGuid));
            episodeOfCareBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            episodeOfCareBuilder.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderConsulting.getResourceId()));
        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientGuid);
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
            episodeOfCareBuilder.setManagingOrganisation(patientReference);

            Reference organizationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
            episodeOfCareBuilder.setManagingOrganisation(organizationReference);

            Reference practitionerReference = imperialHL7Helper.createPractitionerReference(practitionerBuilderConsulting.getResourceId());
            practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            episodeOfCareBuilder.setCareManager(practitionerReference);
        }


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

        //Encounter
    }

    private static void transformADT_A01(FhirResourceFiler fhirResourceFiler, ADT_A01 hapiMsg, String msgType, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A01 adtMsg = hapiMsg;

        //MessageHeader
        MessageHeader fhirMessageHeader = null;
        fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
        //MessageHeader

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();
        practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1().getConsultingDoctor(), practitionerBuilderConsulting);

        PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
        roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();
        practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(adtMsg.getPV1().getReferringDoctor(), practitionerBuilderReferring);

        PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
        roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        //Practitioner

        //LocationOrg
        LocationBuilder locationBuilderOrg = null;
        locationBuilderOrg = new LocationBuilder();
        locationBuilderOrg = LocationTransformer.transformPV1ToOrgLocation(locationBuilderOrg);
        locationBuilderOrg.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
        //LocationOrg

        //LocationPatientAssLoc
        LocationBuilder locationBuilderPatientAssLoc = null;
        locationBuilderPatientAssLoc = new LocationBuilder();
        locationBuilderPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), locationBuilderPatientAssLoc);
        locationBuilderPatientAssLoc.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
        //LocationPatientAssLoc

        //Organization
        organizationBuilder.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));

        fhirResourceFiler.saveAdminResource(null, organizationBuilder);
        //Organization

        //Patient
        PatientBuilder patientBuilder = null;
        CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());
        boolean newPatient = false;
        Patient existingPatient = null;
        existingPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
        if (existingPatient != null) {
            patientBuilder = new PatientBuilder(existingPatient);
        } else {
            patientBuilder = new PatientBuilder();
            imperialHL7Helper.setUniqueId(patientBuilder, patientGuid, null);
            newPatient = true;
        }

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper);

        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderConsulting.getResourceId()));

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);
        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);
            patientBuilder.addCareProvider(organisationReference);

            Reference practitionerReference = imperialHL7Helper.createPractitionerReference(practitionerBuilderConsulting.getResourceId());
            practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerReference);

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
        fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitNum, ResourceType.EpisodeOfCare);
        EpisodeOfCareBuilder episodeOfCareBuilder =null;
        if(fhirEpisodeOfCare == null) {
            // fhirEpisodeOfCare = new EpisodeOfCare();
            episodeOfCareBuilder=new EpisodeOfCareBuilder();
            episodeOfCareBuilder.setId(visitNum);
            newEpisodeOfCare = true;
        }

        else if(fhirEpisodeOfCare != null) {
            episodeOfCareBuilder=new EpisodeOfCareBuilder(fhirEpisodeOfCare);
        }


        episodeOfCareBuilder = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), episodeOfCareBuilder);
        if (newEpisodeOfCare) {
            episodeOfCareBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientGuid));
            episodeOfCareBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            episodeOfCareBuilder.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderConsulting.getResourceId()));
        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientGuid);
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
            episodeOfCareBuilder.setManagingOrganisation(patientReference);

            Reference organizationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
            episodeOfCareBuilder.setManagingOrganisation(organizationReference);

            Reference practitionerReference = imperialHL7Helper.createPractitionerReference(practitionerBuilderConsulting.getResourceId());
            practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            episodeOfCareBuilder.setCareManager(practitionerReference);
        }


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

        //Encounter
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
