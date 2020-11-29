package org.endeavourhealth.transform.hl7v2fhir;

import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v23.datatype.CX;
import ca.uhn.hl7v2.model.v23.datatype.ST;
import ca.uhn.hl7v2.model.v23.datatype.XCN;
import ca.uhn.hl7v2.model.v23.group.ORU_R01_OBSERVATION;
import ca.uhn.hl7v2.model.v23.group.ORU_R01_ORDER_OBSERVATION;
import ca.uhn.hl7v2.model.v23.message.ORU_R01;
import ca.uhn.hl7v2.model.v23.segment.OBR;
import ca.uhn.hl7v2.model.v23.segment.ORC;
import ca.uhn.hl7v2.model.v23.segment.PID;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.hl7v2fhir.helpers.ImperialHL7Helper;
import org.endeavourhealth.transform.hl7v2fhir.transforms.*;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class ImperialHL7FhirORUTransformer {
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

        if("ORU_R01".equalsIgnoreCase(msgType)) {
            ORU_R01 oruMsg = (ORU_R01) hapiMsg;

            //Organization
            OrganizationBuilder organizationBuilder = null;
            organizationBuilder = new OrganizationBuilder();
            organizationBuilder = OrganizationTransformer.transformPV1ToOrganization(organizationBuilder);
            //Organization

            //LocationOrg
            LocationBuilder locationBuilderOrg = null;
            locationBuilderOrg = new LocationBuilder();
            locationBuilderOrg = LocationTransformer.transformPV1ToOrgLocation(locationBuilderOrg);
            locationBuilderOrg.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, locationBuilderOrg);
            //LocationOrg

            //Organization
            organizationBuilder.setMainLocation(ImperialHL7Helper.createReference(ResourceType.Location, locationBuilderOrg.getResourceId()));

            fhirResourceFiler.saveAdminResource(null, organizationBuilder);
            //Organization

            //Patient
            PatientBuilder patientBuilder = null;
            PID pid = oruMsg.getRESPONSE().getPATIENT().getPID();
            CX[] patientIdList = pid.getPatientIDInternalID();
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

            patientBuilder = PatientTransformer.transformPIDToPatient(pid, patientBuilder, fhirResourceFiler, imperialHL7Helper, oruMsg.getMSH().getMessageType().getTriggerEvent().getValue());

            if(newPatient) {
                patientBuilder.setManagingOrganisation(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
                patientBuilder.addCareProvider(ImperialHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));

                fhirResourceFiler.savePatientResource(null, true, patientBuilder);
            } else {
                Reference organisationReference = imperialHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, imperialHL7Helper);
                patientBuilder.setManagingOrganisation(organisationReference);
                patientBuilder.addCareProvider(organisationReference);

                fhirResourceFiler.savePatientResource(null, false, patientBuilder);
            }
            //Patient

            ORC orc = oruMsg.getRESPONSE().getORDER_OBSERVATION().getORC();
            OBR obr = oruMsg.getRESPONSE().getORDER_OBSERVATION().getOBR();
            ORU_R01_ORDER_OBSERVATION orderObserv = oruMsg.getRESPONSE().getORDER_OBSERVATION();

            //Observation
            List<ORU_R01_OBSERVATION> obserVals = orderObserv.getOBSERVATIONAll();
            for (ORU_R01_OBSERVATION val : obserVals) {
                ObservationTransformer.createObservation(pid, oruMsg.getMSH(), obr, orderObserv, fhirResourceFiler, imperialHL7Helper, val);
            }
            //Observation

            //Pathology
            String sendingApplication = oruMsg.getMSH().getSendingApplication().getNamespaceID().getValue();
            if("RYJ_PATH".equalsIgnoreCase(sendingApplication)) {
                //Practitioner
                Practitioner practitioner = null;
                XCN[] orderingProvider = obr.getOrderingProvider();
                ST idNumber = orderingProvider[0].getIDNumber();
                practitioner = (Practitioner) imperialHL7Helper.retrieveResource(idNumber.toString(), ResourceType.Practitioner);
                PractitionerTransformer.transformPathPractitioner(obr, practitioner, fhirResourceFiler);
                //Practitioner
            }
            //Pathology

            //Diagnostic Report
            DiagnosticReportTransformer.createDiagnosticReport(pid, orc, obr, orderObserv, fhirResourceFiler, imperialHL7Helper, oruMsg.getMSH());
            //Diagnostic Report

        }
    }

}