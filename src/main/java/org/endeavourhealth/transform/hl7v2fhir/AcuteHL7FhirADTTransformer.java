package org.endeavourhealth.transform.hl7v2fhir;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v23.datatype.*;
import ca.uhn.hl7v2.model.v23.message.*;
import ca.uhn.hl7v2.model.v23.segment.*;
import com.fasterxml.jackson.databind.JsonNode;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.usermanager.caching.OrganisationCache;
import org.endeavourhealth.core.database.rdbms.datasharingmanager.models.OrganisationEntity;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.fhirhl7v2.FhirHl7v2Filer;
import org.endeavourhealth.transform.hl7v2fhir.helpers.AcuteHL7Helper;
import org.endeavourhealth.transform.hl7v2fhir.transforms.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public abstract class AcuteHL7FhirADTTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(AcuteHL7FhirADTTransformer.class);

    public static void transform(FhirResourceFiler fhirResourceFiler, String source, Message hapiMsg) throws Exception {

        String msgType = (hapiMsg.printStructure()).substring(0, 7);
        AcuteHL7Helper acuteHL7Helper = new AcuteHL7Helper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(),
                fhirResourceFiler.getExchangeId(), null, null);
        if ("ADT_A01".equalsIgnoreCase(msgType)) {
            transformADT_A01(fhirResourceFiler, (ADT_A01) hapiMsg, acuteHL7Helper,source);
        } else if ("ADT_A02".equalsIgnoreCase(msgType)) {
            transformADT_A02(fhirResourceFiler, (ADT_A02) hapiMsg, acuteHL7Helper,source);
        } else if ("ADT_A03".equalsIgnoreCase(msgType)) {
            transformADT_A03(fhirResourceFiler, (ADT_A03) hapiMsg, acuteHL7Helper,source);
        } else if ("ADT_A11".equalsIgnoreCase(msgType)) {
            transformADT_A11(fhirResourceFiler, (ADT_A11) hapiMsg, acuteHL7Helper,source);
        } else if ("ADT_A12".equalsIgnoreCase(msgType)) {
            transformADT_A12(fhirResourceFiler, (ADT_A12) hapiMsg, acuteHL7Helper,source);
        } else if ("ADT_A13".equalsIgnoreCase(msgType)) {
            transformADT_A13(fhirResourceFiler, (ADT_A13) hapiMsg, acuteHL7Helper,source);
        } else if ("ADT_A08".equalsIgnoreCase(msgType)) {
            transformADT_A08(fhirResourceFiler, (ADT_A08) hapiMsg, acuteHL7Helper,source);
        } else if ("ADT_A28".equalsIgnoreCase(msgType)) {
            transformADT_A28(fhirResourceFiler, (ADT_A28) hapiMsg, acuteHL7Helper,source);
        } else if ("ADT_A31".equalsIgnoreCase(msgType)) {
            transformADT_A31(fhirResourceFiler, (ADT_A31) hapiMsg, acuteHL7Helper,source);
        } else if ("ADT_A34".equalsIgnoreCase(msgType)) {
            transformADT_A34(fhirResourceFiler, (ADT_A34) hapiMsg, acuteHL7Helper,source);
        } else if ("ADT_A05".equalsIgnoreCase(msgType)) {
            transformADT_A05(fhirResourceFiler, (ADT_A05) hapiMsg, acuteHL7Helper,source);
        }
    }
    private static void transformADT_A34(FhirResourceFiler fhirResourceFiler, ADT_A34 adtMsg, AcuteHL7Helper acuteHL7Helper, String source) throws Exception {
        String validationError = "";
        String msg_type="A34";
        boolean process_message = true;
        checkMsgConditions(msg_type,adtMsg.getMSH(),adtMsg.getPD1(),adtMsg.getPID(),null,acuteHL7Helper,source);
        validationError = validationError + validatePID(adtMsg.getPID(), false);
        validationError = validationError + validatePD1(null, msg_type);
        validationError = validationError + validateMSH(adtMsg.getMSH());
        validationError = validationError + ValidateADT(adtMsg.getMRG().getMrg1_PriorPatientIDInternal().length==0, "MRG", " Medical Record Number for the patient donor");


        if(validationError.length() > 0){
            ST messageId = null;
            if(!adtMsg.getMSH().getMessageControlID().isEmpty()){ messageId = adtMsg.getMSH().getMessageControlID(); }
             /* String errorMessage ="Mandatory field(s) are missing for  message type = "+ msg_type +", \n Segment="+ validationError +", \n Medical Record Number=" + AcuteHL7Helper.getPatientGUID(adtMsg.getPID()) + ", \n Organization = " + source +
                    ", \n Message ID = " + messageId + " and \n Exchange ID = "+ acuteHL7Helper.getExchangeId().toString();*/

            TransformWarnings.log(LOG,acuteHL7Helper,
                    "Mandatory field(s) are missing for message type={}, Segment={}, MRN={}, MessageID={}",
                    msg_type,validationError, AcuteHL7Helper.getPatientGUID(adtMsg.getPID()),messageId);
          //  raiseSlackAlert(errorMessage);
            if(validationError.contains("Medical record number") || validationError.contains("NHS Number") || validationError.contains("Datetime of message")) {
                process_message=false;
            }
        }

        if(process_message){
                PatientBuilder patientBuilder = null;
                String patientGuid = acuteHL7Helper.getPatientGUID(adtMsg.getPID());
                Patient existingPatient = null;
                existingPatient = (Patient) acuteHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
                if (existingPatient != null) {
                    patientBuilder = new PatientBuilder(existingPatient);
                } else {
                    patientBuilder = new PatientBuilder();
                    acuteHL7Helper.setUniqueId(patientBuilder, patientGuid, null);
                }
                String msgType=adtMsg.getMSH().getMessageType().getTriggerEvent().getValue();
                String msgId=adtMsg.getMSH().getMessageControlID().getValue();
                patientBuilder = PatientTransformer.transformPIDToPatient(adtMsg.getPID(), patientBuilder, fhirResourceFiler, acuteHL7Helper, msgType, source,msgId);
                boolean patientExist = false;

                if (existingPatient == null) {
                    OrganisationEntity org = OrganisationCache.getOrganisationDetailsFromOdsCode(source);
                    String name = org.getName();
                        Reference organizationReference
                                = ReferenceHelper.createReference(ResourceType.Organization, name);
                    patientBuilder.setManagingOrganisation(organizationReference);
                    UUID EdsId = acuteHL7Helper.mapLocalIdToEDSId(patientBuilder.getResource().getId(),ResourceType.Patient);
                    acuteHL7Helper.MapRemainingSourceIdsWithEDSId(AcuteHL7Helper.getRemainingSourceids(adtMsg.getPID(),"Patient"),String.valueOf(EdsId),ResourceType.Patient);
                    fhirResourceFiler.savePatientResource(null, true, patientBuilder);
                    patientExist = true;
                }
                ObservationTransformer.transformObservation(acuteHL7Helper, fhirResourceFiler, adtMsg.getPID(), msgType, patientBuilder, patientGuid,source,msgId);
                FhirHl7v2Filer.AdtResourceFiler filer = new FhirHl7v2Filer.AdtResourceFiler(fhirResourceFiler);
                PatientTransformer.performA34PatientMerge(filer, adtMsg.getPID(), adtMsg.getMRG(), acuteHL7Helper,patientExist);

    }}

    private static void transformADT_A13(FhirResourceFiler fhirResourceFiler, ADT_A13 adtMsg, AcuteHL7Helper acuteHL7Helper, String source) throws Exception {

        boolean process_message = validateHL7Msg("A13", adtMsg.getMSH(), adtMsg.getEVN(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getPID(),acuteHL7Helper, source);
        if (process_message) {
            transformADT_remove(fhirResourceFiler, adtMsg.getPID(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getMSH(),adtMsg.getEVN(), acuteHL7Helper, source, 13);
        }
    }

    private static void transformADT_A11(FhirResourceFiler fhirResourceFiler, ADT_A11 adtMsg, AcuteHL7Helper acuteHL7Helper, String source) throws Exception {

        boolean process_message = validateHL7Msg("A11", adtMsg.getMSH(),adtMsg.getEVN(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getPID(),acuteHL7Helper, source);
        if (process_message) {
            transformADT_remove(fhirResourceFiler, adtMsg.getPID(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getMSH(), adtMsg.getEVN(), acuteHL7Helper, source, 11);
        }
    }

    private static void transformADT_A03(FhirResourceFiler fhirResourceFiler, ADT_A03 adtMsg, AcuteHL7Helper acuteHL7Helper, String source) throws Exception {

        boolean process_message = validateHL7Msg("A03", adtMsg.getMSH(),adtMsg.getEVN(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getPID(),acuteHL7Helper, source);
        if (process_message) {
            transformADT_add(fhirResourceFiler, adtMsg.getPID(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getMSH(), adtMsg.getEVN(), acuteHL7Helper,source);
        }
    }

    private static void transformADT_A28(FhirResourceFiler fhirResourceFiler, ADT_A28 adtMsg, AcuteHL7Helper acuteHL7Helper, String source) throws Exception {

        boolean process_message = validateHL7Msg("A28", adtMsg.getMSH(),adtMsg.getEVN(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getPID(),acuteHL7Helper, source);
        if (process_message) {
            transformADT_add(fhirResourceFiler, adtMsg.getPID(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getMSH(), adtMsg.getEVN(), acuteHL7Helper,source);
        }
    }

    private static void transformADT_A02(FhirResourceFiler fhirResourceFiler, ADT_A02 adtMsg, AcuteHL7Helper acuteHL7Helper, String source) throws Exception {

        boolean process_message = validateHL7Msg("A02", adtMsg.getMSH(),adtMsg.getEVN(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getPID(),acuteHL7Helper, source);
        if (process_message) {
            transformADT_add(fhirResourceFiler, adtMsg.getPID(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getMSH(), adtMsg.getEVN(), acuteHL7Helper,source);
        }
    }

    private static void transformADT_A01(FhirResourceFiler fhirResourceFiler, ADT_A01 adtMsg, AcuteHL7Helper acuteHL7Helper, String source) throws Exception {
        boolean process_message = validateHL7Msg("A01", adtMsg.getMSH(),adtMsg.getEVN(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getPID(),acuteHL7Helper,source);
        if (process_message) {
            transformADT_add(fhirResourceFiler, adtMsg.getPID(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getMSH(), adtMsg.getEVN(), acuteHL7Helper,source);
        }
    }

 /*   private static void raiseSlackAlert(String validationError) {
        LOG.info("validation Error: "+ validationError);
        SlackHelper.sendSlackMessage(SlackHelper.Channel.Hl7v2Receiver, validationError);
    }
*/
    private static void transformADT_A08(FhirResourceFiler fhirResourceFiler, ADT_A08 adtMsg, AcuteHL7Helper acuteHL7Helper, String source) throws Exception {

        boolean process_message = validateHL7Msg("A08", adtMsg.getMSH(),adtMsg.getEVN(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getPID(),acuteHL7Helper, source);
        if (process_message) {
            transformADT_add(fhirResourceFiler, adtMsg.getPID(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getMSH(), adtMsg.getEVN(), acuteHL7Helper,source);
        }
    }

    private static void transformADT_A12(FhirResourceFiler fhirResourceFiler, ADT_A12 adtMsg, AcuteHL7Helper acuteHL7Helper, String source) throws Exception {

        boolean process_message = validateHL7Msg("A12", adtMsg.getMSH(), adtMsg.getEVN(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getPID(),acuteHL7Helper, source);
        if (process_message) {
            transformADT_add(fhirResourceFiler, adtMsg.getPID(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getMSH(),adtMsg.getEVN(), acuteHL7Helper,source);
        }
    }

    private static void transformADT_A31(FhirResourceFiler fhirResourceFiler, ADT_A31 adtMsg, AcuteHL7Helper acuteHL7Helper, String source) throws Exception {
        boolean process_message = validateHL7Msg("A31", adtMsg.getMSH(), adtMsg.getEVN(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getPID(),acuteHL7Helper, source);
        if (process_message) {
            transformADT_add(fhirResourceFiler, adtMsg.getPID(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getMSH(), adtMsg.getEVN(), acuteHL7Helper,source);
        }
    }

    private static void transformADT_A05(FhirResourceFiler fhirResourceFiler, ADT_A05 adtMsg, AcuteHL7Helper acuteHL7Helper, String source) throws Exception {

        boolean process_message = validateHL7Msg("A05", adtMsg.getMSH(), adtMsg.getEVN(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getPID(), acuteHL7Helper, source);
        if (process_message) {
            transformADT_add(fhirResourceFiler, adtMsg.getPID(), adtMsg.getPD1(), adtMsg.getPV1(), adtMsg.getMSH(), adtMsg.getEVN(), acuteHL7Helper,source);
        }
    }

    private static void transformADT_add(FhirResourceFiler fhirResourceFiler, PID pid, PD1 pd1, PV1 pv1, MSH msh, EVN evn, AcuteHL7Helper acuteHL7Helper, String source) throws Exception {
        String msgType=msh.getMessageType().getTriggerEvent().getValue();
        String msgId=msh.getMessageControlID().getValue();
        OrganizationBuilder organizationBuilder = OrganizationTransformer.createHL7OrgBuilder(source);
        LocationBuilder locationBuilderOrg = LocationTransformer.createHL7LocationBuilder(fhirResourceFiler, organizationBuilder,source);
        OrganizationBuilder organizationBuilderDemographic = OrganizationTransformer.transformPD1ToOrganization(pd1);
        OrganizationTransformer.saveOrganizationWithLocation(fhirResourceFiler, organizationBuilder, locationBuilderOrg,organizationBuilderDemographic);
        // GP Practitioner
        PractitionerBuilder practitionerBuilderDemographic = PractitionerTransformer.createAcutePractitioner(fhirResourceFiler, pd1.getPatientPrimaryCareProviderNameIDNo(), organizationBuilderDemographic);
        PatientBuilder patientBuilder = PatientTransformer.transformPatient(acuteHL7Helper, practitionerBuilderDemographic, organizationBuilder, organizationBuilderDemographic, fhirResourceFiler, pid, msgType,source,msgId);
        ObservationTransformer.transformObservation(acuteHL7Helper, fhirResourceFiler, pid, msgType, patientBuilder, AcuteHL7Helper.getPatientGUID(pid),source,msgId);

        EncounterTransformer.transformPV1ToEncounter(pv1,evn, pid.getPatientAccountNumber(), fhirResourceFiler, acuteHL7Helper, msgType, AcuteHL7Helper.getPatientGUID(pid),source,msgId);
        PractitionerBuilder practitionerBuilderConsulting = PractitionerTransformer.createAcutePractitioner(fhirResourceFiler, pv1.getConsultingDoctor(), organizationBuilder);
        PractitionerTransformer.createAcutePractitioner(fhirResourceFiler, pv1.getReferringDoctor(), organizationBuilder);
        EpisodeOfCareTransformer.transformEpisodeOfCare(acuteHL7Helper, fhirResourceFiler, pv1, practitionerBuilderConsulting, organizationBuilder, AcuteHL7Helper.getPatientGUID(pid), false);

        LocationTransformer.savePatientLocation(fhirResourceFiler, organizationBuilder, pv1);

    }

    private static void transformADT_remove(FhirResourceFiler fhirResourceFiler, PID pid, PD1 pd1, PV1 pv1, MSH msh,EVN evn, AcuteHL7Helper acuteHL7Helper, String source, int msgNum) throws Exception {
        String msgType=msh.getMessageType().getTriggerEvent().getValue();
        String msgId=msh.getMessageControlID().getValue();

        OrganizationBuilder organizationBuilder = OrganizationTransformer.createHL7OrgBuilder(source);
        LocationBuilder locationBuilderOrg = LocationTransformer.createHL7LocationBuilder(fhirResourceFiler, organizationBuilder,source);
        OrganizationBuilder organizationBuilderDemographic = OrganizationTransformer.transformPD1ToOrganization(pd1);
        OrganizationTransformer.saveOrganizationWithLocation(fhirResourceFiler, organizationBuilder, locationBuilderOrg,organizationBuilderDemographic);
        // GP Practitioner
        PractitionerBuilder practitionerBuilderDemographic = PractitionerTransformer.createAcutePractitioner(fhirResourceFiler, pd1.getPatientPrimaryCareProviderNameIDNo(), organizationBuilderDemographic);
        PatientBuilder patientBuilder = PatientTransformer.transformPatient(acuteHL7Helper, practitionerBuilderDemographic, organizationBuilder, organizationBuilderDemographic, fhirResourceFiler, pid, msgType, source,msgId);
        ObservationTransformer.transformObservation(acuteHL7Helper, fhirResourceFiler, pid, msgType, patientBuilder, AcuteHL7Helper.getPatientGUID(pid),source,msgId);
        PractitionerBuilder practitionerBuilderConsulting = PractitionerTransformer.createAcutePractitioner(fhirResourceFiler, pv1.getConsultingDoctor(), organizationBuilder);
        PractitionerTransformer.createAcutePractitioner(fhirResourceFiler, pv1.getReferringDoctor(), organizationBuilder);
        LocationTransformer.savePatientLocation(fhirResourceFiler, organizationBuilder, pv1);

        // Episode Of care and Encounter
        if (msgNum == 13) {
            EpisodeOfCareTransformer.transformEpisodeOfCare(acuteHL7Helper, fhirResourceFiler, pv1, practitionerBuilderConsulting, organizationBuilder, AcuteHL7Helper.getPatientGUID(pid), true);
            Encounter existingParentEncounter
                    = (Encounter) acuteHL7Helper.retrieveResourceForLocalId(ResourceType.Encounter, String.valueOf(pv1.getVisitNumber().getID()));
            if (existingParentEncounter != null && existingParentEncounter.hasContained()) {
                EncounterTransformer.undoChildEncounter(existingParentEncounter, acuteHL7Helper, fhirResourceFiler);
            } else {
                EncounterTransformer.transformPV1ToEncounter(pv1,evn, pid.getPatientAccountNumber(), fhirResourceFiler, acuteHL7Helper, msgType, AcuteHL7Helper.getPatientGUID(pid),source,msgId);
            }
        } else if (msgNum == 11) {
            Encounter existingParentEncounter
                    = (Encounter) acuteHL7Helper.retrieveResourceForLocalId(ResourceType.Encounter, String.valueOf(pv1.getVisitNumber().getID()));
            if (existingParentEncounter != null) {
                EncounterTransformer.deleteEncounterAndChildren(pv1, fhirResourceFiler, acuteHL7Helper, msh.getMessageType().getTriggerEvent().getValue(),msgId);
            }

            String visitNum = String.valueOf(pv1.getVisitNumber().getID());
            EpisodeOfCare episodeOfCare = (EpisodeOfCare) acuteHL7Helper.retrieveResource(visitNum, ResourceType.EpisodeOfCare);
            if (episodeOfCare != null) {
                EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(episodeOfCare);
                fhirResourceFiler.deletePatientResource(null, false, episodeOfCareBuilder);
            }
        }
    }
    private static void checkMsgConditions(String msg_type, MSH msh, PD1 pd1, PID pid, PV1 pv1,AcuteHL7Helper acuteHL7Helper,String source) throws Exception {
        String validation_error = "";
        JsonNode configAcute = ConfigManager.getConfigurationAsJson("acute_msg_configs");
        if (null != configAcute && null != configAcute.get(source)) {
            if (configAcute.get(source).get("GP_CODE") != null && pd1 != null) {
                XCN xcn = pd1.getPatientPrimaryCareProviderNameIDNo(0);
                if(!xcn.getXcn1_IDNumber().isEmpty()){
                    validation_error = validation_error + ValidateADT(xcn.getXcn1_IDNumber().toString().contains(configAcute.get(source).get("GP_CODE").asText()),
                            "PD1", " GP Code is a local code for KCH, code is  " + xcn.getXcn1_IDNumber().toString().replace(configAcute.get(source).get("GP_CODE").asText(),""));
                }
            }
            if (configAcute.get(source).get("CONSULTANT_CODE") != null && pv1 != null) {
                XCN xcn = pv1.getPv19_ConsultingDoctor(0);
                if(!xcn.getXcn1_IDNumber().isEmpty()){
                    validation_error = validation_error + ValidateADT(xcn.getXcn1_IDNumber().toString().contains(configAcute.get(source).get("CONSULTANT_CODE").asText()),
                            " PV1", " Consulting clinician is a local code for KCH, code is  " + xcn.getXcn1_IDNumber().toString().replace(configAcute.get(source).get("CONSULTANT_CODE").asText(),""));
                }
            }

            if (validation_error.length() > 0) {
                ST messageId = null;
                if (!msh.getMessageControlID().isEmpty()) {
                    messageId = msh.getMessageControlID();
                }

               /* String errorMessage = "Message flagged for validation checks for message type = " + msg_type + ", \n Segment=" + validation_error + ", \n Medical Record Number=" + AcuteHL7Helper.getPatientGUID(pid) + ", \n Organization = " + source +
                        ", \n Message ID = " + messageId + " and \n Exchange ID = " + acuteHL7Helper.getExchangeId().toString();*/

                String mrn = AcuteHL7Helper.getPatientGUID(pid);
                if (mrn == null) {
                    mrn = "Not Available";
                }
                TransformWarnings.log(LOG, acuteHL7Helper,
                        "Message flagged for validation checks for message type={}, Segment={}, MRN={}, MessageID={}",
                        msg_type, validation_error, mrn, messageId);
              //  raiseSlackAlert(errorMessage);
            }
        }
    }

    private static boolean validateHL7Msg(String msg_type, MSH msh, EVN evn, PD1 pd1, PV1 pv1, PID pid, AcuteHL7Helper acuteHL7Helper, String source) throws Exception {
        String validation_error = "";
        boolean process_message=true;
        checkMsgConditions(msg_type,msh,pd1,pid,pv1,acuteHL7Helper,source);

        validation_error = validation_error + validateMSH(msh);
        validation_error = validation_error + validateEVN(evn,msg_type);
        validation_error = validation_error + validatePID(pid, (!pv1.getPv119_VisitNumber().isEmpty()));
        validation_error = validation_error + validatePV1(pv1, msg_type);
        validation_error = validation_error + validatePD1(pd1, msg_type);

        if (validation_error.length() > 0) {
            ST messageId = null;
            if(!msh.getMessageControlID().isEmpty()){ messageId = msh.getMessageControlID(); };

          /*  String errorMessage ="Mandatory field(s) are missing for  message type = "+ msg_type +", \n Segment="+ validation_error +", \n Medical Record Number=" + AcuteHL7Helper.getPatientGUID(pid) + ", \n Organization = " + source +
                                    ", \n Message ID = " + messageId + " and \n Exchange ID = "+ acuteHL7Helper.getExchangeId().toString();*/

            String mrn = AcuteHL7Helper.getPatientGUID(pid);
            if(mrn == null){
                mrn = "Not Available";
            }
            TransformWarnings.log(LOG,acuteHL7Helper,
                    "Mandatory field(s) are missing for message type={}, Segment={}, MRN={}, MessageID={}",
                    msg_type,validation_error,mrn,messageId);
         //   raiseSlackAlert(errorMessage);
            if(validation_error.contains("Medical record number") || validation_error.contains("Datetime of message")){
                process_message = false;
            }
        }
        return process_message;
    }

    private static String validateEVN(EVN evn,String msg_type) throws Exception{
        String validation_error = "";
        String segment = "EVN";
        if (msg_type.equalsIgnoreCase("A02")) {
            validation_error = validation_error + ValidateADT(evn.getEvn6_EventOccurred().isEmpty(), segment, " Event Occurred");
        }
        return validation_error;
    }

    private static String validateMSH(MSH msh) throws Exception{
        String validation_error = "";
        String segment = "MSH";
        validation_error = validation_error + ValidateADT(msh.getFieldSeparator().isEmpty(), segment, " Field Separator");
        validation_error = validation_error + ValidateADT(msh.getEncodingCharacters().isEmpty(), segment, " Encoding characters");
        validation_error = validation_error + ValidateADT(msh.getSendingApplication().isEmpty(), segment, " Sending application");
        validation_error = validation_error + ValidateADT(msh.getSendingFacility().isEmpty(), segment, " Sending facility");
        validation_error = validation_error + ValidateADT(msh.getReceivingApplication().isEmpty(), segment, " Receiving application");
        validation_error = validation_error + ValidateADT(msh.getReceivingFacility().isEmpty(), segment, " Receiving facility");
        validation_error = validation_error + ValidateADT(msh.getDateTimeOfMessage().isEmpty(), segment, " Datetime of message");
        CM_MSG msg_type_Obj = msh.getMessageType();
        validation_error = validation_error + ValidateADT(msg_type_Obj.getCm_msg1_MessageType().isEmpty(), segment, " Message type");
        validation_error = validation_error + ValidateADT(msg_type_Obj.getCm_msg2_TriggerEvent().isEmpty(), segment, " Event type");
        validation_error = validation_error + ValidateADT(msh.getMessageControlID().isEmpty(), segment, " Messaging control ID");
        validation_error = validation_error + ValidateADT(msh.getVersionID().isEmpty(), segment, " Version ID");
        return validation_error;
    }

    private static String validatePID(PID pid, boolean hasEncounter) throws HL7Exception{
        String validation_error = "";
        String segment = "PID";
        CX cx = AcuteHL7Helper.getPatientReferenceSegment(pid,"MRN");
        if(cx==null) {
            validation_error = validation_error + ValidateADT(true, segment, " Medical record number");
            validation_error = validation_error + ValidateADT(true, segment, " Assigning authority");
        } else {
            validation_error = validation_error + ValidateADT(cx.getID().isEmpty(), segment, " Medical record number");
            validation_error = validation_error + ValidateADT(cx.getAssigningAuthority().isEmpty(), segment, " Assigning authority");
        }
        validation_error = validation_error + ValidateADT(pid.getPatientName(0).getFamilyName().isEmpty(), segment, "  Surname");
        validation_error = validation_error + ValidateADT(pid.getDateOfBirth().isEmpty(), segment, " Date of birth");
        validation_error = validation_error + ValidateADT(pid.getSex().isEmpty(), segment, " Sex");
        if (!pid.getPatientDeathIndicator().isEmpty() && pid.getPatientDeathIndicator().toString().equalsIgnoreCase("Y")) {
            validation_error = validation_error + ValidateADT(pid.getPatientDeathDateAndTime().isEmpty(), segment, " Date and time of death");
        }
        return validation_error;
    }

    private static String validatePV1(PV1 pv1, String msg_type) throws HL7Exception{
        String validation_error = "";
        String segment = "PV1";
        if (!(msg_type.equalsIgnoreCase("A28")
                || msg_type.equalsIgnoreCase("A31") || msg_type.equalsIgnoreCase("A34")|| msg_type.equalsIgnoreCase("A11"))) {
            if (msg_type.equalsIgnoreCase("A01")) {

                validation_error = validation_error + ValidateADT(pv1.getPv14_AdmissionType().isEmpty(), segment, " Admission method/type");
                validation_error = validation_error + ValidateADT(pv1.getPv114_AdmitSource().isEmpty(), segment, " Admission source");
            } else if (msg_type.equalsIgnoreCase("A03")) {
                validation_error = validation_error + ValidateADT(pv1.getPv136_DischargeDisposition().isEmpty(), segment, " Discharge disposition");
                validation_error = validation_error + ValidateADT(pv1.getPv137_DischargedToLocation().isEmpty(), segment, " Discharge location");
                validation_error = validation_error + ValidateADT(pv1.getPv145_DischargeDateTime().isEmpty(), segment, "Date and time of discharge");
            }
            validation_error = validation_error + ValidateADT(pv1.getPatientClass().isEmpty(), segment, " Patient class");
            PL pl = pv1.getPv13_AssignedPatientLocation();
            validation_error = validation_error + ValidateADT(pl.getPl1_PointOfCare().isEmpty(), segment, " Location");
            XCN xcn = pv1.getPv19_ConsultingDoctor(0);
            validation_error = validation_error + ValidateADT(xcn.getXcn1_IDNumber().isEmpty(), segment, " Consulting clinician");
            validation_error = validation_error + ValidateADT(xcn.getXcn2_FamilyName().isEmpty(), segment, " Consulting clinician family name");
            validation_error = validation_error + ValidateADT(pv1.getHospitalService().isEmpty(), segment, " Hospital service (specialty)");
            validation_error = validation_error + ValidateADT(pv1.getPv118_PatientType().isEmpty(), segment, " Encounter class");
            validation_error = validation_error + ValidateADT(pv1.getPv119_VisitNumber().isEmpty(), segment, " Encounter ID");
            if (!(msg_type.equalsIgnoreCase("A12"))) {
                validation_error = validation_error + ValidateADT(pv1.getPv144_AdmitDateTime().isEmpty(), segment, " Date and time of admission");
            }
        }

        return validation_error;
    }

    private static String validatePD1(PD1 pd1, String msg_type) throws HL7Exception {
        String validation_error = "";
        String segment = "PD1";
        if (!(msg_type.equalsIgnoreCase("A34"))) {
            validation_error = validation_error + ValidateADT(pd1.getPd13_PatientPrimaryFacility()[0].getOrganizationName().isEmpty(), segment, " Patient GP Practice Organisation Name");
            validation_error = validation_error + ValidateADT(pd1.getPd13_PatientPrimaryFacility()[0].getIDNumber().isEmpty(), segment, " Patient GP Practice ID ");
        }
        return validation_error;
    }

    private static String ValidateADT(boolean flag, String segment, String element) {
        if (flag) {
            return segment + "." + element;
        }
        return "";
    }
}
