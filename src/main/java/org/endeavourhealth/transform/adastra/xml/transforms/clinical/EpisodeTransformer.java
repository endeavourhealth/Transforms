package org.endeavourhealth.transform.adastra.xml.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.transform.adastra.AdastraXmlHelper;
import org.endeavourhealth.transform.adastra.xml.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.XmlDateHelper;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Period;

import java.util.Date;

import static org.endeavourhealth.transform.adastra.AdastraXmlHelper.uniqueIdMapper;

public class EpisodeTransformer {

    public static void transform(AdastraCaseDataExport caseReport, FhirResourceFiler fhirResourceFiler) throws Exception {

        EpisodeOfCare fhirEpisode = new EpisodeOfCare();
        fhirEpisode.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_EPISODE_OF_CARE));

        fhirEpisode.setId(caseReport.getPatient().getNationalNumber().getNumber() + ":" + caseReport.getAdastraCaseReference());
        uniqueIdMapper.put("episode", fhirEpisode.getId());

        fhirEpisode.setPatient(AdastraXmlHelper.createPatientReference());
        fhirEpisode.setManagingOrganization(AdastraXmlHelper.createOrganisationReference(caseReport.getPatient().getGpRegistration().getSurgeryNationalCode()));


        Date caseStart = XmlDateHelper.convertDate(caseReport.getActiveDate());
        Period fhirPeriod = PeriodHelper.createPeriod(caseStart, null);
        fhirEpisode.setPeriod(fhirPeriod);

        if (caseReport.getCompletedDate() != null) {
            //if we have a completion date, the care is finish
            fhirEpisode.setStatus(EpisodeOfCare.EpisodeOfCareStatus.FINISHED);

            Date caseEnd = XmlDateHelper.convertDate(caseReport.getCompletedDate());
            fhirPeriod.setEnd(caseEnd);

        } else {
            //if we don't have a completion date, the case is still active. although this should never happen
            fhirEpisode.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
        }

        fhirResourceFiler.savePatientResource(null, new EpisodeOfCareBuilder(fhirEpisode));
    }
}
