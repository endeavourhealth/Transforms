package org.endeavourhealth.transform.hl7v2fhir;

import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v23.datatype.CX;
import ca.uhn.hl7v2.model.v23.message.ORU_R01;
import ca.uhn.hl7v2.model.v23.segment.OBR;
import ca.uhn.hl7v2.model.v23.segment.OBX;
import ca.uhn.hl7v2.model.v23.segment.ORC;
import ca.uhn.hl7v2.model.v23.segment.PID;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.endeavourhealth.transform.hl7v2fhir.transforms.DiagnosticReportTransformer;
import org.endeavourhealth.transform.hl7v2fhir.transforms.LocationTransformer;
import org.endeavourhealth.transform.hl7v2fhir.transforms.OrganizationTransformer;
import org.endeavourhealth.transform.hl7v2fhir.transforms.PatientTransformer;
import org.hl7.fhir.instance.model.Location;
import org.hl7.fhir.instance.model.Organization;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ImperialHL7FhirORUTransformer {
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

        if("ORU_R01".equalsIgnoreCase(msgType)) {
            ORU_R01 oruMsg = (ORU_R01) hapiMsg;

            //Organization
            Organization fhirOrganization = null;
            fhirOrganization = new Organization();
            fhirOrganization = OrganizationTransformer.transformPV1ToOrganization(fhirOrganization);
            //Organization

            //LocationOrg
            Location fhirLocationOrg = null;
            fhirLocationOrg = new Location();
            fhirLocationOrg = LocationTransformer.transformPV1ToOrgLocation(fhirLocationOrg);
            fhirLocationOrg.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));

            LocationBuilder locationBuilderOrg = new LocationBuilder(fhirLocationOrg);
            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //Organization
            fhirOrganization.addExtension().setValue(ImperialHL7Helper.createReference(ResourceType.Location, fhirLocationOrg.getId()));

            OrganizationBuilder organizationBuilder = new OrganizationBuilder(fhirOrganization);
            fhirResourceFiler.saveAdminResource(null, organizationBuilder);
            //Organization

            //Patient
            PID pid = oruMsg.getRESPONSE().getPATIENT().getPID();
            CX[] patientIdList = pid.getPatientIDInternalID();
            String patientGuid = String.valueOf(patientIdList[0].getID());
            boolean newPatient = false;
            Patient fhirPatient = null;
            fhirPatient = (Patient) imperialHL7Helper.retrieveResource(patientGuid, ResourceType.Patient);
            if(fhirPatient == null) {
                fhirPatient = new Patient();
                fhirPatient.setId(patientGuid);
                newPatient = true;
            }
            fhirPatient = PatientTransformer.transformPIDToPatient(pid, fhirPatient);

            if (newPatient) {
                fhirPatient.setManagingOrganization(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, fhirOrganization.getId()));
                /*fhirPatient.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Practitioner, fhirPractitioner.getId()));*/
            } else {
                /*Reference organizationReference = imperialHL7Helper.createOrganizationReference(fhirOrganization.getId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                fhirPatient.setManagingOrganization(organizationReference);
                fhirPatient.addCareProvider(organizationReference);

                Reference practitionerReference = imperialHL7Helper.createPractitionerReference(fhirPractitioner.getId());
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                fhirPatient.addCareProvider(practitionerReference);*/
            }

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);
            if(newPatient) {
                fhirResourceFiler.savePatientResource(null, true, patientBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            }
            //Patient

            //Diagnostic Report
            ORC orc = oruMsg.getRESPONSE().getORDER_OBSERVATION().getORC();
            OBR obr = oruMsg.getRESPONSE().getORDER_OBSERVATION().getOBR();
            OBX obx = oruMsg.getRESPONSE().getORDER_OBSERVATION().getOBSERVATION().getOBX();

            orc.getOrderStatus();
            orc.getPlacerGroupNumber();
            orc.getPlacerOrderNumber();
            orc.getFillerOrderNumber();

            DiagnosticReportTransformer.createOrDeleteDiagnosticReport(pid, obr, fhirResourceFiler, imperialHL7Helper);
            //Diagnostic Report

        }
    }

}