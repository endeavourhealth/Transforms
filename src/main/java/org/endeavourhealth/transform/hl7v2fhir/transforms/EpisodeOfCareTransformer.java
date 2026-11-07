package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.CX;
import ca.uhn.hl7v2.model.v23.datatype.TS;
import ca.uhn.hl7v2.model.v23.segment.PV1;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.hl7v2fhir.helpers.AcuteHL7Helper;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EpisodeOfCareTransformer {

    /**
     *
     * @param pv1
     * @param episodeOfCare
     * @return
     * @throws Exception
     */
    public static EpisodeOfCareBuilder transformPV1ToEpisodeOfCare(PV1 pv1, EpisodeOfCareBuilder episodeOfCare) throws Exception {

        // episodeOfCare.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_EPISODE_OF_CARE));

        IdentifierBuilder identifierBuilder = new IdentifierBuilder(episodeOfCare);
        identifierBuilder.setSystem("");
        identifierBuilder.setValue(pv1.getVisitNumber().getID().toString());

        episodeOfCare.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

        TS admitDtTime = pv1.getAdmitDateTime();
        String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());
        if(startDt != null) {
            Date stDt = formatter.parse(startDt.substring(0, 4) + "-" + startDt.substring(4, 6) + "-" + startDt.substring(6, 8));
            episodeOfCare.setRegistrationStartDate(stDt);
        }

        TS dischargeDtTime = pv1.getDischargeDateTime();
        String endDt = String.valueOf(dischargeDtTime.getTimeOfAnEvent());
        if(endDt != null) {
            Date dsDt = formatter.parse(endDt.substring(0,4)+"-"+endDt.substring(4,6)+"-"+endDt.substring(6,8));
            episodeOfCare.setRegistrationEndDate(dsDt);
        }

        return episodeOfCare;
    }

    public static void transformEpisodeOfCare(AcuteHL7Helper acuteHL7Helper, FhirResourceFiler fhirResourceFiler, PV1 pv1, PractitionerBuilder practitionerBuilderConsulting, OrganizationBuilder organizationBuilder, String patientGuid ,boolean undo) throws Exception {
        //EpisodeOfCare
        boolean newEpisodeOfCare = false;
        EpisodeOfCare fhirEpisodeOfCare = null;
        CX visitNum = pv1.getVisitNumber();
        if(visitNum.getID().getValue() != null) {
            String visitId = String.valueOf(visitNum.getID());
            fhirEpisodeOfCare = (EpisodeOfCare)acuteHL7Helper.retrieveResource(visitId, ResourceType.EpisodeOfCare);
            EpisodeOfCareBuilder episodeOfCareBuilder =null;
            if(fhirEpisodeOfCare == null) {
                episodeOfCareBuilder=new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(visitId);
                newEpisodeOfCare = true;
            } else if(fhirEpisodeOfCare != null) {
                episodeOfCareBuilder=new EpisodeOfCareBuilder(fhirEpisodeOfCare);
                if(undo) {
                    episodeOfCareBuilder.setRegistrationEndDate(null);
                }

            }
            episodeOfCareBuilder = EpisodeOfCareTransformer.transformPV1ToEpisodeOfCare(pv1, episodeOfCareBuilder);
            if (newEpisodeOfCare) {
                episodeOfCareBuilder.setPatient(AcuteHL7Helper.createReference(ResourceType.Patient, patientGuid));
                episodeOfCareBuilder.setManagingOrganisation(AcuteHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
               if (practitionerBuilderConsulting.getResourceId()!=null) {
                   episodeOfCareBuilder.setCareManager(AcuteHL7Helper.createReference(ResourceType.Practitioner, practitionerBuilderConsulting.getResourceId()));
               }
            }
            else if(!undo) {
                Reference patientReference = acuteHL7Helper.createPatientReference(patientGuid);
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                episodeOfCareBuilder.setPatient(patientReference);
                Reference organizationReference = acuteHL7Helper.createOrganizationReference(organizationBuilder.getResourceId());
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, fhirResourceFiler);
                episodeOfCareBuilder.setManagingOrganisation(organizationReference);
                if (practitionerBuilderConsulting.getResourceId()!=null) {
                    Reference practitionerReference = acuteHL7Helper.createPractitionerReference(practitionerBuilderConsulting.getResourceId());
                    practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
                    episodeOfCareBuilder.setCareManager(practitionerReference);
                }
            }

            if(newEpisodeOfCare) {
                fhirResourceFiler.savePatientResource(null, true, episodeOfCareBuilder);
            } else {
                fhirResourceFiler.savePatientResource(null, false, episodeOfCareBuilder);
            }
        }
    }

}
