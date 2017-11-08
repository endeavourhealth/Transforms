package org.endeavourhealth.transform.adastra.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.common.XmlDateHelper;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Period;
import org.hl7.fhir.instance.model.Resource;

import java.util.Date;
import java.util.List;

import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.guidMapper;

public class EpisodeTransformer {

    public static void transform(AdastraCaseDataExport caseReport, List<Resource> resources) {

        EpisodeOfCare fhirEpisode = new EpisodeOfCare();
        fhirEpisode.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_EPISODE_OF_CARE));

        AdastraHelper.setUniqueId(fhirEpisode, caseReport.getAdastraCaseReference());
        guidMapper.put("episode", fhirEpisode.getId());

        fhirEpisode.setPatient(AdastraHelper.createPatientReference());
        fhirEpisode.setManagingOrganization(AdastraHelper.createOrganisationReference(caseReport.getPatient().getGpRegistration().getSurgeryNationalCode()));


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

        resources.add(fhirEpisode);
    }
}
