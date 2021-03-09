package org.endeavourhealth.transform.hl7v2fhir;

import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v23.datatype.*;
import ca.uhn.hl7v2.model.v23.message.*;
import ca.uhn.hl7v2.model.v23.segment.MSH;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformConstant;
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
            transformADT_A01(fhirResourceFiler, (ADT_A01) hapiMsg, imperialHL7Helper);

        } else if("ADT_A02".equalsIgnoreCase(msgType)) {
            transformADT_A02(fhirResourceFiler, (ADT_A02) hapiMsg, imperialHL7Helper);

        } else if("ADT_A03".equalsIgnoreCase(msgType)) {
            transformADT_A03(fhirResourceFiler, (ADT_A03) hapiMsg, imperialHL7Helper);

        } else if("ADT_A11".equalsIgnoreCase(msgType)) {
            transformADT_A11(fhirResourceFiler, (ADT_A11) hapiMsg, imperialHL7Helper);

        } else if("ADT_A12".equalsIgnoreCase(msgType)) {
            transformADT_A12(fhirResourceFiler, (ADT_A12) hapiMsg, imperialHL7Helper);

        } else if("ADT_A13".equalsIgnoreCase(msgType)) {
            transformADT_A13(fhirResourceFiler, (ADT_A13) hapiMsg, imperialHL7Helper);

        } /*else if("ADT_A05".equalsIgnoreCase(msgType)) {
            transformADT_A05(fhirResourceFiler, (ADT_A05) hapiMsg, imperialHL7Helper);

        }*/ else if("ADT_A08".equalsIgnoreCase(msgType)) {
            transformADT_A08(fhirResourceFiler, (ADT_A08) hapiMsg, imperialHL7Helper);

        } else if("ADT_A28".equalsIgnoreCase(msgType)) {
            transformADT_A28(fhirResourceFiler, (ADT_A28) hapiMsg, imperialHL7Helper);

        } else if("ADT_A31".equalsIgnoreCase(msgType)) {
            transformADT_A31(fhirResourceFiler, (ADT_A31) hapiMsg, imperialHL7Helper);

        } else if("ADT_A34".equalsIgnoreCase(msgType)) {
            transformADT_A34(fhirResourceFiler, (ADT_A34) hapiMsg, imperialHL7Helper);
        }
    }

    /**
     *
     * @param fhirResourceFiler
     * @param hapiMsg
     * @throws Exception
     */
    private static void transformADT_A34(FhirResourceFiler fhirResourceFiler, ADT_A34 hapiMsg, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A34 adtMsg = hapiMsg;

        //Patient
        PatientBuilder patientBuilder = null;
        CX[] patientIdList = adtMsg.getPID().getPatientIDInternalID();
        String patientGuid = String.valueOf(patientIdList[0].getID());
        Patient existingPatient = null;
        existingPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
        if (existingPatient != null) {
            patientBuilder = new PatientBuilder(existingPatient);
        } else {
            patientBuilder = new PatientBuilder();
            imperialHL7Helper.setUniqueId(patientBuilder, patientGuid, null);
        }

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if (existingPatient == null) {
            Reference organizationReference
                    = ReferenceHelper.createReference(ResourceType.Organization, "Imperial College Healthcare NHS Trust");
            patientBuilder.setManagingOrganisation(organizationReference);
            fhirResourceFiler.savePatientResource(null, true, patientBuilder);
        }
        //Observation
        ObservationBuilder observationBuilder = null;
        boolean newObservation = false;
        Observation existingObservation = null;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Religion", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Religion", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDToObservation(adtMsg.getPID(), observationBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }
        //Observation

        newObservation = false;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Language", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Language", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDPrimaryLanguageToObservation(adtMsg.getPID(), observationBuilder, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }
        //Observation

        FhirHl7v2Filer.AdtResourceFiler filer = new FhirHl7v2Filer.AdtResourceFiler(fhirResourceFiler);
        PatientTransformer.performA34PatientMerge(filer, adtMsg.getPID(), adtMsg.getMRG(), imperialHL7Helper);
    }



    /**
     *
     * @param fhirResourceFiler
     * @param hapiMsg
     * @param imperialHL7Helper
     * @throws Exception
     */
    private static void transformADT_A31(FhirResourceFiler fhirResourceFiler, ADT_A31 hapiMsg, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A31 adtMsg = hapiMsg;
        //MessageHeader
        MessageHeader fhirMessageHeader = null;
        fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
        //MessageHeader

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);

        OrganizationBuilder organizationBuilderDemographic = null;
        organizationBuilderDemographic = new OrganizationBuilder();
        organizationBuilderDemographic = OrganizationTransformer.transformPD1ToOrganization(adtMsg.getPD1(), organizationBuilderDemographic);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();

        XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
        if(consultingDoctor != null && consultingDoctor.length > 0) {
            practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(consultingDoctor, practitionerBuilderConsulting);

            PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
            roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);
        }

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();

        XCN[] referringDoctor = adtMsg.getPV1().getReferringDoctor();
        if(referringDoctor != null && referringDoctor.length > 0) {
            practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(referringDoctor, practitionerBuilderReferring);

            PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
            roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        }

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
        String assignedPatientLoc = String.valueOf(adtMsg.getPV1().getAssignedPatientLocation().getPointOfCare());
        if(assignedPatientLoc != null) {
            locationBuilderPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), locationBuilderPatientAssLoc);
            locationBuilderPatientAssLoc.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
        }

        //LocationPatientAssLoc

        //Organization
        organizationBuilder.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));
        fhirResourceFiler.saveAdminResource(null, organizationBuilder);

        //Organization - GP Practice code
        //organizationBuilderDemographic.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));
        fhirResourceFiler.saveAdminResource(null, organizationBuilderDemographic);
        //Organization

        // GP Practitioner
        PractitionerBuilder practitionerBuilderDemographic = null;
        practitionerBuilderDemographic = new PractitionerBuilder();
        XCN[] primaryCareDoctor = adtMsg.getPD1().getPatientPrimaryCareProviderNameIDNo();
        if(primaryCareDoctor != null && primaryCareDoctor.length > 0) {
            practitionerBuilderDemographic = PractitionerTransformer.transformPV1ToPractitioner(primaryCareDoctor, practitionerBuilderDemographic);

            PractitionerRoleBuilder roleBuilderDemographic = new PractitionerRoleBuilder(practitionerBuilderDemographic);
            roleBuilderDemographic.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization,
                    organizationBuilderDemographic.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderDemographic);
        }

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

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        Reference practitionerAddCareProviderReference = null;
        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            practitionerAddCareProviderReference= imperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);
        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);

            practitionerAddCareProviderReference = imperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            practitionerAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);

            Reference OrgAddCareProviderReference= imperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId());
            OrgAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(OrgAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(OrgAddCareProviderReference);

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
        //Patient

        //Observation
        ObservationBuilder observationBuilder = null;
        boolean newObservation = false;
        Observation existingObservation = null;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Religion", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Religion", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDToObservation(adtMsg.getPID(), observationBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            //patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }
        //Observation

        newObservation = false;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Language", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Language", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDPrimaryLanguageToObservation(adtMsg.getPID(), observationBuilder, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }


        //EpisodeOfCare
        boolean newEpisodeOfCare = false;
        EpisodeOfCare fhirEpisodeOfCare = null;

        CX visitNum = adtMsg.getPV1().getVisitNumber();
        if(visitNum.getID().getValue() != null) {
            String visitId = String.valueOf(visitNum.getID());
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitId, ResourceType.EpisodeOfCare);
            EpisodeOfCareBuilder episodeOfCareBuilder =null;
            if(fhirEpisodeOfCare == null) {
                episodeOfCareBuilder=new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(visitId);
                newEpisodeOfCare = true;
            } else if(fhirEpisodeOfCare != null) {
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
        }
        //EpisodeOfCare

        //Encounter
        EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), adtMsg.getPID().getPatientAccountNumber(), fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue(), patientGuid);
        //Encounter
    }

    /**
     *
     * @param fhirResourceFiler
     * @param hapiMsg
     * @param imperialHL7Helper
     * @throws Exception
     */
    private static void transformADT_A28(FhirResourceFiler fhirResourceFiler, ADT_A28 hapiMsg, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A28 adtMsg = hapiMsg;

        //MessageHeader
        MessageHeader fhirMessageHeader = null;
        fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
        //MessageHeader

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);

        OrganizationBuilder organizationBuilderDemographic = null;
        organizationBuilderDemographic = new OrganizationBuilder();
        organizationBuilderDemographic = OrganizationTransformer.transformPD1ToOrganization(adtMsg.getPD1(), organizationBuilderDemographic);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();

        XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
        if(consultingDoctor != null && consultingDoctor.length > 0) {
            practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(consultingDoctor, practitionerBuilderConsulting);

            PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
            roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);
        }

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();

        XCN[] referringDoctor = adtMsg.getPV1().getReferringDoctor();
        if(referringDoctor != null && referringDoctor.length > 0) {
            practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(referringDoctor, practitionerBuilderReferring);

            PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
            roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        }


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
        ST assignedPatientLoc = adtMsg.getPV1().getAssignedPatientLocation().getLocationType();
        if(assignedPatientLoc.getValue() != null) {
            locationBuilderPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(adtMsg.getPV1(), locationBuilderPatientAssLoc);
            locationBuilderPatientAssLoc.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
        }
        //LocationPatientAssLoc

        //Organization
        organizationBuilder.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));
        fhirResourceFiler.saveAdminResource(null, organizationBuilder);

        //organizationBuilderDemographic.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));
        fhirResourceFiler.saveAdminResource(null, organizationBuilderDemographic);
        //Organization

        // GP Practitioner
        PractitionerBuilder practitionerBuilderDemographic = null;
        practitionerBuilderDemographic = new PractitionerBuilder();
        XCN[] primaryCareDoctor = adtMsg.getPD1().getPatientPrimaryCareProviderNameIDNo();
        if(primaryCareDoctor != null && primaryCareDoctor.length > 0) {
            practitionerBuilderDemographic = PractitionerTransformer.transformPV1ToPractitioner(primaryCareDoctor, practitionerBuilderDemographic);

            PractitionerRoleBuilder roleBuilderDemographic = new PractitionerRoleBuilder(practitionerBuilderDemographic);
            roleBuilderDemographic.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization,
                    organizationBuilderDemographic.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderDemographic);
        }
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

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        Reference practitionerAddCareProviderReference = null;
        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            //patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId()));
            practitionerAddCareProviderReference = ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);

        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);

            practitionerAddCareProviderReference = imperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            practitionerAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);

            Reference OrgAddCareProviderReference= imperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId());
            OrgAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(OrgAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(OrgAddCareProviderReference);

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
        //Patient

        //Observation
        ObservationBuilder observationBuilder = null;
        boolean newObservation = false;
        Observation existingObservation = null;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Religion", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Religion", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDToObservation(adtMsg.getPID(), observationBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }
        //Observation

        newObservation = false;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Language", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Language", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDPrimaryLanguageToObservation(adtMsg.getPID(), observationBuilder, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }


        //EpisodeOfCare
        boolean newEpisodeOfCare = false;
        EpisodeOfCare fhirEpisodeOfCare = null;

        CX visitNum = adtMsg.getPV1().getVisitNumber();
        if(visitNum.getID().getValue() != null) {
            String visitId = String.valueOf(visitNum.getID());
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitId, ResourceType.EpisodeOfCare);
            EpisodeOfCareBuilder episodeOfCareBuilder =null;
            if(fhirEpisodeOfCare == null) {
                episodeOfCareBuilder=new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(visitId);
                newEpisodeOfCare = true;
            } else if(fhirEpisodeOfCare != null) {
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
        }
        //EpisodeOfCare

        //Encounter
        EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), adtMsg.getPID().getPatientAccountNumber(), fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue(), patientGuid);
        //Encounter
    }

    /**
     *
     * @param fhirResourceFiler
     * @param hapiMsg
     * @param imperialHL7Helper
     * @throws Exception
     */
    private static void transformADT_A08(FhirResourceFiler fhirResourceFiler, ADT_A08 hapiMsg, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A08 adtMsg = hapiMsg;

        //MessageHeader
        MessageHeader fhirMessageHeader = null;
        fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
        //MessageHeader

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);

        OrganizationBuilder organizationBuilderDemographic = null;
        organizationBuilderDemographic = new OrganizationBuilder();
        organizationBuilderDemographic = OrganizationTransformer.transformPD1ToOrganization(adtMsg.getPD1(), organizationBuilderDemographic);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();

        XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
        if(consultingDoctor != null && consultingDoctor.length > 0) {
            practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(consultingDoctor, practitionerBuilderConsulting);

            PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
            roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);
        }

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();

        XCN[] referringDoctor = adtMsg.getPV1().getReferringDoctor();
        if(referringDoctor != null && referringDoctor.length > 0) {
            practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(referringDoctor, practitionerBuilderReferring);

            PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
            roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        }


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

        //organizationBuilderDemographic.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));
        fhirResourceFiler.saveAdminResource(null, organizationBuilderDemographic);
        //Organization

        // GP Practitioner
        PractitionerBuilder practitionerBuilderDemographic = null;
        practitionerBuilderDemographic = new PractitionerBuilder();
        XCN[] primaryCareDoctor = adtMsg.getPD1().getPatientPrimaryCareProviderNameIDNo();
        if(primaryCareDoctor != null && primaryCareDoctor.length > 0) {
            practitionerBuilderDemographic = PractitionerTransformer.transformPV1ToPractitioner(primaryCareDoctor, practitionerBuilderDemographic);

            PractitionerRoleBuilder roleBuilderDemographic = new PractitionerRoleBuilder(practitionerBuilderDemographic);
            roleBuilderDemographic.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderDemographic);
        }

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

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        Reference practitionerAddCareProviderReference = null;
        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            //patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId()));
            practitionerAddCareProviderReference = ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);

        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);

            practitionerAddCareProviderReference = imperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            practitionerAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);

            Reference OrgAddCareProviderReference= imperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId());
            OrgAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(OrgAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(OrgAddCareProviderReference);

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
        //Patient

        //Observation
        ObservationBuilder observationBuilder = null;
        boolean newObservation = false;
        Observation existingObservation = null;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Religion", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Religion", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDToObservation(adtMsg.getPID(), observationBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }
        //Observation

        newObservation = false;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Language", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Language", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDPrimaryLanguageToObservation(adtMsg.getPID(), observationBuilder, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }


        //EpisodeOfCare
        boolean newEpisodeOfCare = false;
        EpisodeOfCare fhirEpisodeOfCare = null;
        CX visitNum = adtMsg.getPV1().getVisitNumber();
        if(visitNum.getID().getValue() != null) {
            String visitId = String.valueOf(visitNum.getID());
            String sourceEpisodeId = ImperialHL7Helper.createUniqueId(patientGuid, visitId);
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitId, ResourceType.EpisodeOfCare);
            EpisodeOfCareBuilder episodeOfCareBuilder =null;
            if(fhirEpisodeOfCare == null) {
                episodeOfCareBuilder=new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(visitId);
                newEpisodeOfCare = true;
            } else if(fhirEpisodeOfCare != null) {
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
        }
        //EpisodeOfCare

        //Encounter
        EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), adtMsg.getPID().getPatientAccountNumber(), fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue(), patientGuid);
        //Encounter
    }

    /**
     *
     * @param fhirResourceFiler
     * @param hapiMsg
     * @param imperialHL7Helper
     * @throws Exception
     */
    private static void transformADT_A13(FhirResourceFiler fhirResourceFiler, ADT_A13 hapiMsg, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A13 adtMsg = hapiMsg;

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);

        OrganizationBuilder organizationBuilderDemographic = null;
        organizationBuilderDemographic = new OrganizationBuilder();
        organizationBuilderDemographic = OrganizationTransformer.transformPD1ToOrganization(adtMsg.getPD1(), organizationBuilderDemographic);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();

        XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
        if(consultingDoctor != null && consultingDoctor.length > 0) {
            practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(consultingDoctor, practitionerBuilderConsulting);

            PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
            roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);
        }

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();

        XCN[] referringDoctor = adtMsg.getPV1().getReferringDoctor();
        if(referringDoctor != null && referringDoctor.length > 0) {
            practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(referringDoctor, practitionerBuilderReferring);

            PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
            roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        }


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

        //organizationBuilderDemographic.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));
        fhirResourceFiler.saveAdminResource(null, organizationBuilderDemographic);
        //Organization

        // GP Practitioner
        PractitionerBuilder practitionerBuilderDemographic = null;
        practitionerBuilderDemographic = new PractitionerBuilder();
        XCN[] primaryCareDoctor = adtMsg.getPD1().getPatientPrimaryCareProviderNameIDNo();
        if(primaryCareDoctor != null && primaryCareDoctor.length > 0) {
            practitionerBuilderDemographic = PractitionerTransformer.transformPV1ToPractitioner(primaryCareDoctor, practitionerBuilderDemographic);

            PractitionerRoleBuilder roleBuilderDemographic = new PractitionerRoleBuilder(practitionerBuilderDemographic);
            roleBuilderDemographic.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderDemographic);
        }
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

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        Reference practitionerAddCareProviderReference = null;
        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            //patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId()));
            practitionerAddCareProviderReference = ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);

        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);

            practitionerAddCareProviderReference = imperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            practitionerAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);

            Reference OrgAddCareProviderReference= imperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId());
            OrgAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(OrgAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(OrgAddCareProviderReference);

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
        //Patient

        //Observation
        ObservationBuilder observationBuilder = null;
        boolean newObservation = false;
        Observation existingObservation = null;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Religion", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Religion", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDToObservation(adtMsg.getPID(), observationBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }
        //Observation

        newObservation = false;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Language", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Language", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDPrimaryLanguageToObservation(adtMsg.getPID(), observationBuilder, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }

        //EpisodeOfCare
        boolean newEpisodeOfCare = false;
        EpisodeOfCare fhirEpisodeOfCare = null;
        CX visitNum = adtMsg.getPV1().getVisitNumber();

        if(visitNum.getID().getValue() != null) {
            String visitId = String.valueOf(visitNum.getID());
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitId, ResourceType.EpisodeOfCare);
            EpisodeOfCareBuilder episodeOfCareBuilder =null;
            if(fhirEpisodeOfCare == null) {
                episodeOfCareBuilder=new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(visitId);
                newEpisodeOfCare = true;
            } else if(fhirEpisodeOfCare != null) {
                episodeOfCareBuilder=new EpisodeOfCareBuilder(fhirEpisodeOfCare);
                episodeOfCareBuilder.setRegistrationEndDate(null);

            }

            episodeOfCareBuilder = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(adtMsg.getPV1(), episodeOfCareBuilder);
            if (newEpisodeOfCare) {
                episodeOfCareBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientGuid));
                episodeOfCareBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
                episodeOfCareBuilder.setCareManager(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderConsulting.getResourceId()));
            }

            if(newEpisodeOfCare) {
                fhirResourceFiler.savePatientResource(null, true, episodeOfCareBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, episodeOfCareBuilder);
            }
        }
        //EpisodeOfCare

        //Encounter
        Encounter existingParentEncounter
                = (Encounter) imperialHL7Helper.retrieveResourceForLocalId(ResourceType.Encounter, String.valueOf(adtMsg.getPV1().getVisitNumber().getID()));

        EncounterBuilder parentEncounterBuilder = new EncounterBuilder(existingParentEncounter);

        if (existingParentEncounter!= null && existingParentEncounter.hasContained()) {
            ContainedListBuilder listBuilder = new ContainedListBuilder(parentEncounterBuilder);
            ResourceDalI resourceDal = DalProvider.factoryResourceDal();

            for (List_.ListEntryComponent item : listBuilder.getContainedListItems()) {

                Reference ref = item.getItem();
                ReferenceComponents comps = ReferenceHelper.getReferenceComponents(ref);
                if (comps.getResourceType() != ResourceType.Encounter) {
                    continue;
                }
                Encounter childEncounter
                        = (Encounter) resourceDal.getCurrentVersionAsResource(imperialHL7Helper.getServiceId(), ResourceType.Encounter, comps.getId());

                if (childEncounter != null) {
                    EncounterBuilder cBuilder = new EncounterBuilder(childEncounter);

                    if (listBuilder.getContainedListItems().size() == 1) {
                        cBuilder.getPeriod().setEnd(null);
                        fhirResourceFiler.savePatientResource(null, false, cBuilder);
                    } else {
                        if (cBuilder.getPeriod().getEnd()!=null) {
                            fhirResourceFiler.deletePatientResource(null, false, cBuilder);
                        }
                    }
                }
            }
            EncounterTransformer.deleteEndDate(existingParentEncounter, fhirResourceFiler);
        }
        else {
            EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), adtMsg.getPID().getPatientAccountNumber(), fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue(), patientGuid);
        }
        //Encounter
    }

    /**
     *
     * @param fhirResourceFiler
     * @param hapiMsg
     * @param imperialHL7Helper
     * @throws Exception
     */
    private static void transformADT_A12(FhirResourceFiler fhirResourceFiler, ADT_A12 hapiMsg, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A12 adtMsg = hapiMsg;

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);

        OrganizationBuilder organizationBuilderDemographic = null;
        organizationBuilderDemographic = new OrganizationBuilder();
        organizationBuilderDemographic = OrganizationTransformer.transformPD1ToOrganization(adtMsg.getPD1(), organizationBuilderDemographic);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();

        XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
        if(consultingDoctor != null && consultingDoctor.length > 0) {
            practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(consultingDoctor, practitionerBuilderConsulting);

            PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
            roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);
        }

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();

        XCN[] referringDoctor = adtMsg.getPV1().getReferringDoctor();
        if(referringDoctor != null && referringDoctor.length > 0) {
            practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(referringDoctor, practitionerBuilderReferring);

            PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
            roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        }

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

        //organizationBuilderDemographic.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));
        fhirResourceFiler.saveAdminResource(null, organizationBuilderDemographic);
        //Organization

        // GP Practitioner
        PractitionerBuilder practitionerBuilderDemographic = null;
        practitionerBuilderDemographic = new PractitionerBuilder();
        XCN[] primaryCareDoctor = adtMsg.getPD1().getPatientPrimaryCareProviderNameIDNo();
        if(primaryCareDoctor != null && primaryCareDoctor.length > 0) {
            practitionerBuilderDemographic = PractitionerTransformer.transformPV1ToPractitioner(primaryCareDoctor, practitionerBuilderDemographic);

            PractitionerRoleBuilder roleBuilderDemographic = new PractitionerRoleBuilder(practitionerBuilderDemographic);
            roleBuilderDemographic.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderDemographic);
        }
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

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        Reference practitionerAddCareProviderReference = null;
        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            //patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId()));
            practitionerAddCareProviderReference = ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);

        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);

            practitionerAddCareProviderReference = imperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            practitionerAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);

            Reference OrgAddCareProviderReference= imperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId());
            OrgAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(OrgAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(OrgAddCareProviderReference);

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
        //Patient

        //Observation
        ObservationBuilder observationBuilder = null;
        boolean newObservation = false;
        Observation existingObservation = null;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Religion", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Religion", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDToObservation(adtMsg.getPID(), observationBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }
        //Observation

        newObservation = false;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Language", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Language", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDPrimaryLanguageToObservation(adtMsg.getPID(), observationBuilder, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }


        //EpisodeOfCare
        boolean newEpisodeOfCare = false;
        EpisodeOfCare fhirEpisodeOfCare = null;

        CX visitNum = adtMsg.getPV1().getVisitNumber();
        if(visitNum.getID().getValue() != null) {
            String visitId = String.valueOf(visitNum.getID());
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitId, ResourceType.EpisodeOfCare);
            EpisodeOfCareBuilder episodeOfCareBuilder =null;
            if(fhirEpisodeOfCare == null) {
                episodeOfCareBuilder=new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(visitId);
                newEpisodeOfCare = true;
            } else if(fhirEpisodeOfCare != null) {
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
        }
        //EpisodeOfCare

        //Encounter
        EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), adtMsg.getPID().getPatientAccountNumber(), fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue(), patientGuid);
        //Encounter
    }

    /**
     *
     * @param fhirResourceFiler
     * @param hapiMsg
     * @param imperialHL7Helper
     * @throws Exception
     */
    private static void transformADT_A11(FhirResourceFiler fhirResourceFiler, ADT_A11 hapiMsg, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A11 adtMsg = hapiMsg;

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);

        OrganizationBuilder organizationBuilderDemographic = null;
        organizationBuilderDemographic = new OrganizationBuilder();
        organizationBuilderDemographic = OrganizationTransformer.transformPD1ToOrganization(adtMsg.getPD1(), organizationBuilderDemographic);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();

        XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
        if(consultingDoctor != null && consultingDoctor.length > 0) {
            practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(consultingDoctor, practitionerBuilderConsulting);

            PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
            roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);
        }

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();

        XCN[] referringDoctor = adtMsg.getPV1().getReferringDoctor();
        if(referringDoctor != null && referringDoctor.length > 0) {
            practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(referringDoctor, practitionerBuilderReferring);

            PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
            roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        }

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

        // organizationBuilderDemographic.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));
        fhirResourceFiler.saveAdminResource(null, organizationBuilderDemographic);
        //Organization

        // GP Practitioner
        PractitionerBuilder practitionerBuilderDemographic = null;
        practitionerBuilderDemographic = new PractitionerBuilder();
        XCN[] primaryCareDoctor = adtMsg.getPD1().getPatientPrimaryCareProviderNameIDNo();
        if(primaryCareDoctor != null && primaryCareDoctor.length > 0) {
            practitionerBuilderDemographic = PractitionerTransformer.transformPV1ToPractitioner(primaryCareDoctor, practitionerBuilderDemographic);

            PractitionerRoleBuilder roleBuilderDemographic = new PractitionerRoleBuilder(practitionerBuilderDemographic);
            roleBuilderDemographic.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderDemographic);
        }
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

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        Reference practitionerAddCareProviderReference = null;
        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            //patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId()));
            practitionerAddCareProviderReference = ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);


        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);

            practitionerAddCareProviderReference = imperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            practitionerAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);

            Reference OrgAddCareProviderReference= imperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId());
            OrgAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(OrgAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(OrgAddCareProviderReference);

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
        //Patient

        //Observation
        ObservationBuilder observationBuilder = null;
        boolean newObservation = false;
        Observation existingObservation = null;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Religion", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Religion", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDToObservation(adtMsg.getPID(), observationBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            //patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }
        //Observation

        newObservation = false;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Language", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Language", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDPrimaryLanguageToObservation(adtMsg.getPID(), observationBuilder, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }


        //Encounter
        Encounter existingParentEncounter
                = (Encounter) imperialHL7Helper.retrieveResourceForLocalId(ResourceType.Encounter, String.valueOf(adtMsg.getPV1().getVisitNumber().getID()));
        if(existingParentEncounter != null) {
            EncounterTransformer.deleteEncounterAndChildren(adtMsg.getPV1(), fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        }
        //Encounter

        //EpisodeOfCare
        String visitNum = String.valueOf(adtMsg.getPV1().getVisitNumber().getID());
        EpisodeOfCare episodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitNum, ResourceType.EpisodeOfCare);
        if(episodeOfCare != null) {
            EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(episodeOfCare);
            fhirResourceFiler.deletePatientResource(null, false, episodeOfCareBuilder);
        }
        //EpisodeOfCare
    }

    /**
     *
     * @param fhirResourceFiler
     * @param hapiMsg
     * @param imperialHL7Helper
     * @throws Exception
     */
    private static void transformADT_A03(FhirResourceFiler fhirResourceFiler, ADT_A03 hapiMsg, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A03 adtMsg = hapiMsg;

        //MessageHeader
        MessageHeader fhirMessageHeader = null;
        fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
        //MessageHeader

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);

        OrganizationBuilder organizationBuilderDemographic = null;
        organizationBuilderDemographic = new OrganizationBuilder();
        organizationBuilderDemographic = OrganizationTransformer.transformPD1ToOrganization(adtMsg.getPD1(), organizationBuilderDemographic);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();
        XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
        if(consultingDoctor != null && consultingDoctor.length > 0) {
            practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(consultingDoctor, practitionerBuilderConsulting);

            PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
            roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);
        }

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();
        XCN[] referringDoctor = adtMsg.getPV1().getReferringDoctor();
        if(referringDoctor != null && referringDoctor.length > 0) {
            practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(referringDoctor, practitionerBuilderReferring);

            PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
            roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        }

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

        //organizationBuilderDemographic.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));
        fhirResourceFiler.saveAdminResource(null, organizationBuilderDemographic);
        //Organization

        // GP Practitioner
        PractitionerBuilder practitionerBuilderDemographic = null;
        practitionerBuilderDemographic = new PractitionerBuilder();
        XCN[] primaryCareDoctor = adtMsg.getPD1().getPatientPrimaryCareProviderNameIDNo();
        if(primaryCareDoctor != null && primaryCareDoctor.length > 0) {
            practitionerBuilderDemographic = PractitionerTransformer.transformPV1ToPractitioner(primaryCareDoctor, practitionerBuilderDemographic);

            PractitionerRoleBuilder roleBuilderDemographic = new PractitionerRoleBuilder(practitionerBuilderDemographic);
            roleBuilderDemographic.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderDemographic);
        }
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

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        Reference practitionerAddCareProviderReference = null;
        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            //patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId()));
            practitionerAddCareProviderReference = ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);

        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);

            practitionerAddCareProviderReference = imperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            practitionerAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);

            Reference OrgAddCareProviderReference= imperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId());
            OrgAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(OrgAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(OrgAddCareProviderReference);

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
        //Patient

        //Observation
        ObservationBuilder observationBuilder = null;
        boolean newObservation = false;
        Observation existingObservation = null;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Religion", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Religion", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDToObservation(adtMsg.getPID(), observationBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }
        //Observation

        newObservation = false;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Language", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Language", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDPrimaryLanguageToObservation(adtMsg.getPID(), observationBuilder, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }


        //EpisodeOfCare
        boolean newEpisodeOfCare = false;
        EpisodeOfCare fhirEpisodeOfCare = null;

        CX visitNum = adtMsg.getPV1().getVisitNumber();
        if(visitNum.getID().getValue() != null) {
            String visitId = String.valueOf(visitNum.getID());
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitId, ResourceType.EpisodeOfCare);
            EpisodeOfCareBuilder episodeOfCareBuilder =null;
            if(fhirEpisodeOfCare == null) {
                episodeOfCareBuilder=new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(visitId);
                newEpisodeOfCare = true;
            } else if(fhirEpisodeOfCare != null) {
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
        }
        //EpisodeOfCare

        //Encounter
        EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), adtMsg.getPID().getPatientAccountNumber(), fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue(), patientGuid);
        //Encounter
    }

    /**
     *
     * @param fhirResourceFiler!
     * @param hapiMsg
     * @param imperialHL7Helper
     * @throws Exception
     */
    private static void transformADT_A02(FhirResourceFiler fhirResourceFiler, ADT_A02 hapiMsg, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A02 adtMsg = hapiMsg;

        //MessageHeader
        MessageHeader fhirMessageHeader = null;
        fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
        //MessageHeader

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);

        OrganizationBuilder organizationBuilderDemographic = null;
        organizationBuilderDemographic = new OrganizationBuilder();
        organizationBuilderDemographic = OrganizationTransformer.transformPD1ToOrganization(adtMsg.getPD1(), organizationBuilderDemographic);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();
        XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
        if(consultingDoctor != null && consultingDoctor.length > 0) {
            practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(consultingDoctor, practitionerBuilderConsulting);

            PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
            roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);
        }

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();
        XCN[] referringDoctor = adtMsg.getPV1().getReferringDoctor();
        if(referringDoctor != null && referringDoctor.length > 0) {
            practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(referringDoctor, practitionerBuilderReferring);

            PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
            roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        }

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

        //organizationBuilderDemographic.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));
        fhirResourceFiler.saveAdminResource(null, organizationBuilderDemographic);
        //Organization

        // GP Practitioner
        PractitionerBuilder practitionerBuilderDemographic = null;
        practitionerBuilderDemographic = new PractitionerBuilder();
        XCN[] primaryCareDoctor = adtMsg.getPD1().getPatientPrimaryCareProviderNameIDNo();
        if(primaryCareDoctor != null && primaryCareDoctor.length > 0) {
            practitionerBuilderDemographic = PractitionerTransformer.transformPV1ToPractitioner(primaryCareDoctor, practitionerBuilderDemographic);

            PractitionerRoleBuilder roleBuilderDemographic = new PractitionerRoleBuilder(practitionerBuilderDemographic);
            roleBuilderDemographic.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderDemographic);
        }
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

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        Reference practitionerAddCareProviderReference = null;
        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            //patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId()));
            practitionerAddCareProviderReference = ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);

        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);

            practitionerAddCareProviderReference = imperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            practitionerAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);

            Reference OrgAddCareProviderReference= imperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId());
            OrgAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(OrgAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(OrgAddCareProviderReference);

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
        //Patient

        //Observation
        ObservationBuilder observationBuilder = null;
        boolean newObservation = false;
        Observation existingObservation = null;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Religion", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Religion", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDToObservation(adtMsg.getPID(), observationBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            //patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }
        //Observation

        newObservation = false;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Language", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Language", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDPrimaryLanguageToObservation(adtMsg.getPID(), observationBuilder, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }
        //Observation


        //EpisodeOfCare
        boolean newEpisodeOfCare = false;
        EpisodeOfCare fhirEpisodeOfCare = null;

        CX visitNum = adtMsg.getPV1().getVisitNumber();
        if(visitNum.getID().getValue() != null) {
            String visitId = String.valueOf(visitNum.getID());
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitId, ResourceType.EpisodeOfCare);
            EpisodeOfCareBuilder episodeOfCareBuilder =null;
            if(fhirEpisodeOfCare == null) {
                episodeOfCareBuilder=new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(visitId);
                newEpisodeOfCare = true;
            } else if(fhirEpisodeOfCare != null) {
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
        }
        //EpisodeOfCare

        //Encounter
        EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), adtMsg.getPID().getPatientAccountNumber(), fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue(), patientGuid);
        //Encounter
    }

    /**
     *
     * @param fhirResourceFiler
     *
     * @param hapiMsg
     * @param imperialHL7Helper
     * @throws Exception
     */
    private static void transformADT_A01(FhirResourceFiler fhirResourceFiler, ADT_A01 hapiMsg, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A01 adtMsg = hapiMsg;

        //MessageHeader
        MessageHeader fhirMessageHeader = null;
        fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
        //MessageHeader

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);

        OrganizationBuilder organizationBuilderDemographic = null;
        organizationBuilderDemographic = new OrganizationBuilder();
        organizationBuilderDemographic = OrganizationTransformer.transformPD1ToOrganization(adtMsg.getPD1(), organizationBuilderDemographic);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();
        XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
        if(consultingDoctor != null && consultingDoctor.length > 0) {
            practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(consultingDoctor, practitionerBuilderConsulting);

            PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
            roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);
        }

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();
        XCN[] referringDoctor = adtMsg.getPV1().getReferringDoctor();
        if(referringDoctor != null && referringDoctor.length > 0) {
            practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(referringDoctor, practitionerBuilderReferring);

            PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
            roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        }

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

        //organizationBuilderDemographic.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));
        fhirResourceFiler.saveAdminResource(null, organizationBuilderDemographic);
        //Organization

        // GP Practitioner
        PractitionerBuilder practitionerBuilderDemographic = null;
        practitionerBuilderDemographic = new PractitionerBuilder();
        XCN[] primaryCareDoctor = adtMsg.getPD1().getPatientPrimaryCareProviderNameIDNo();
        if(primaryCareDoctor != null && primaryCareDoctor.length > 0) {
            practitionerBuilderDemographic = PractitionerTransformer.transformPV1ToPractitioner(primaryCareDoctor, practitionerBuilderDemographic);

            PractitionerRoleBuilder roleBuilderDemographic = new PractitionerRoleBuilder(practitionerBuilderDemographic);
            roleBuilderDemographic.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderDemographic);
        }
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

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        Reference practitionerAddCareProviderReference = null;
        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            //patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId()));
            practitionerAddCareProviderReference = ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);

        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);

            practitionerAddCareProviderReference = imperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            practitionerAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);

            Reference OrgAddCareProviderReference= imperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId());
            OrgAddCareProviderReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(OrgAddCareProviderReference, imperialHL7Helper);
            patientBuilder.addCareProvider(OrgAddCareProviderReference);

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
        //Patient

        //Observation
        ObservationBuilder observationBuilder = null;
        boolean newObservation = false;
        Observation existingObservation = null;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Religion", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Religion", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDToObservation(adtMsg.getPID(), observationBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }
        //Observation

        newObservation = false;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Language", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Language", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDPrimaryLanguageToObservation(adtMsg.getPID(), observationBuilder, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }


        //EpisodeOfCare
        boolean newEpisodeOfCare = false;
        EpisodeOfCare fhirEpisodeOfCare = null;

        CX visitNum = adtMsg.getPV1().getVisitNumber();
        if(visitNum.getID().getValue() != null) {
            String visitId = String.valueOf(visitNum.getID());
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitId, ResourceType.EpisodeOfCare);
            EpisodeOfCareBuilder episodeOfCareBuilder =null;
            if(fhirEpisodeOfCare == null) {
                episodeOfCareBuilder=new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(visitId);
                newEpisodeOfCare = true;
            } else if(fhirEpisodeOfCare != null) {
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
        }
        //EpisodeOfCare

        //Encounter
        EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), adtMsg.getPID().getPatientAccountNumber(), fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue(), patientGuid);
        //Encounter
    }

    /**
     *
     * @param fhirResourceFiler
     * @param hapiMsg
     * @param imperialHL7Helper
     * @throws Exception
     */
    private static void transformADT_A05(FhirResourceFiler fhirResourceFiler, ADT_A05 hapiMsg, ImperialHL7Helper imperialHL7Helper) throws Exception {
        ADT_A05 adtMsg = hapiMsg;

        //MessageHeader
        MessageHeader fhirMessageHeader = null;
        fhirMessageHeader = transformPIDToMsgHeader(adtMsg.getMSH());
        //MessageHeader

        //Organization
        OrganizationBuilder organizationBuilder = null;
        organizationBuilder = new OrganizationBuilder();
        organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);

        OrganizationBuilder organizationBuilderDemographic = null;
        organizationBuilderDemographic = new OrganizationBuilder();
        organizationBuilderDemographic = OrganizationTransformer.transformPD1ToOrganization(adtMsg.getPD1(), organizationBuilderDemographic);
        //Organization

        //Practitioner
        PractitionerBuilder practitionerBuilderConsulting = null;
        practitionerBuilderConsulting = new PractitionerBuilder();
        XCN[] consultingDoctor = adtMsg.getPV1().getConsultingDoctor();
        if(consultingDoctor != null && consultingDoctor.length > 0) {
            practitionerBuilderConsulting = PractitionerTransformer.transformPV1ToPractitioner(consultingDoctor, practitionerBuilderConsulting);

            PractitionerRoleBuilder roleBuilderConsulting = new PractitionerRoleBuilder(practitionerBuilderConsulting);
            roleBuilderConsulting.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderConsulting);
        }

        PractitionerBuilder practitionerBuilderReferring = null;
        practitionerBuilderReferring = new PractitionerBuilder();
        XCN[] referringDoctor = adtMsg.getPV1().getReferringDoctor();
        if(referringDoctor != null && referringDoctor.length > 0) {
            practitionerBuilderReferring = PractitionerTransformer.transformPV1ToPractitioner(referringDoctor, practitionerBuilderReferring);

            PractitionerRoleBuilder roleBuilderReferring = new PractitionerRoleBuilder(practitionerBuilderReferring);
            roleBuilderReferring.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderReferring);
        }

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

        //organizationBuilderDemographic.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));
        fhirResourceFiler.saveAdminResource(null, organizationBuilderDemographic);
        //Organization

        // GP Practitioner
        PractitionerBuilder practitionerBuilderDemographic = null;
        practitionerBuilderDemographic = new PractitionerBuilder();
        XCN[] primaryCareDoctor = adtMsg.getPD1().getPatientPrimaryCareProviderNameIDNo();
        if(primaryCareDoctor != null && primaryCareDoctor.length > 0) {
            practitionerBuilderDemographic = PractitionerTransformer.transformPV1ToPractitioner(primaryCareDoctor, practitionerBuilderDemographic);

            PractitionerRoleBuilder roleBuilderDemographic = new PractitionerRoleBuilder(practitionerBuilderDemographic);
            roleBuilderDemographic.setRoleManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, practitionerBuilderDemographic);
        }
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

        patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());

        Reference practitionerAddCareProviderReference = null;
        if(newPatient) {
            patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            //patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId()));
            practitionerAddCareProviderReference = ImperialHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderDemographic.getResourceId());
            patientBuilder.addCareProvider(practitionerAddCareProviderReference);
            patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilderDemographic.getResourceId()));

            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, true, patientBuilder);

        } else {
            Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
            patientBuilder.setManagingOrganisation(organisationReference);

            practitionerAddCareProviderReference = imperialHL7Helper.createReference(ResourceType.Practitioner,
                    practitionerBuilderDemographic.getResourceId());
            //add in additional extended data as Parameters resource with additional extension
            addPractitioner(patientBuilder, practitionerAddCareProviderReference );

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
        //Patient

        //Observation
        ObservationBuilder observationBuilder = null;
        boolean newObservation = false;
        Observation existingObservation = null;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Religion", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Religion", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDToObservation(adtMsg.getPID(), observationBuilder, fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }
        //Observation
        newObservation = false;
        existingObservation = (Observation) imperialHL7Helper.retrieveResource(patientGuid+"Language", ResourceType.Observation);
        if (existingObservation != null) {
            observationBuilder = new ObservationBuilder(existingObservation);
        } else {
            observationBuilder = new ObservationBuilder();
            imperialHL7Helper.setUniqueId(observationBuilder, patientGuid+"Language", null);
            newObservation = true;
        }

        observationBuilder = ObservationTransformer.transformPIDPrimaryLanguageToObservation(adtMsg.getPID(), observationBuilder, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue());
        if(newObservation) {
            observationBuilder.setPatient(ImperialHL7Helper.createReference(ResourceType.Patient, patientBuilder.getResourceId()));
            fhirResourceFiler.savePatientResource(null, true, observationBuilder);

        } else {
            Reference patientReference = imperialHL7Helper.createPatientReference(patientBuilder.getResourceId());
            // patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, imperialHL7Helper);
            observationBuilder.setPatient(patientReference);

            fhirResourceFiler.savePatientResource(null, false, observationBuilder);
        }


        //EpisodeOfCare
        boolean newEpisodeOfCare = false;
        EpisodeOfCare fhirEpisodeOfCare = null;

        CX visitNum = adtMsg.getPV1().getVisitNumber();
        if(visitNum.getID().getValue() != null) {
            String visitId = String.valueOf(visitNum.getID());
            fhirEpisodeOfCare = (EpisodeOfCare)imperialHL7Helper.retrieveResource(visitId, ResourceType.EpisodeOfCare);
            EpisodeOfCareBuilder episodeOfCareBuilder =null;
            if(fhirEpisodeOfCare == null) {
                episodeOfCareBuilder=new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(visitId);
                newEpisodeOfCare = true;
            } else if(fhirEpisodeOfCare != null) {
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
        }
        //EpisodeOfCare

        //Encounter
        EncounterTransformer.transformPV1ToEncounter(adtMsg.getPV1(), adtMsg.getPID().getPatientAccountNumber(), fhirResourceFiler, imperialHL7Helper, adtMsg.getMSH().getMessageType().getTriggerEvent().getValue(), patientGuid);
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

    public static void addPractitioner(PatientBuilder patientBuilder, Reference reference ) {

        //add in additional extended data as Parameters resource with additional extension
        ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(patientBuilder);
        containedParametersBuilder.removeContainedParameters();
        containedParametersBuilder.addParameter(TransformConstant.PARAMETER_GP_PRACTITIONER_ID,
                reference);

    }

}
