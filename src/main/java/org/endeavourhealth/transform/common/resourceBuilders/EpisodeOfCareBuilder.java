package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class EpisodeOfCareBuilder extends ResourceBuilderBase {

    private EpisodeOfCare episodeOfCare = null;

    public EpisodeOfCareBuilder() {
        this(null);
    }

    public EpisodeOfCareBuilder(EpisodeOfCare episodeOfCare) {
        this.episodeOfCare = episodeOfCare;
        if (this.episodeOfCare == null) {
            this.episodeOfCare = new EpisodeOfCare();
            this.episodeOfCare.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_EPISODE_OF_CARE));
        }
    }

    public void setPatient(Reference referenceValue, CsvCell... sourceCells) {
        this.episodeOfCare.setPatient(referenceValue);

        auditValue("patient.reference", sourceCells);
    }

    @Override
    public DomainResource getResource() {
        return episodeOfCare;
    }

    public void setManagingOrganisation(Reference organisationReference, CsvCell... sourceCells) {
        this.episodeOfCare.setManagingOrganization(organisationReference);

        auditValue("managingOrganization.reference", sourceCells);
    }

    public void setCareManager(Reference practitionerReference, CsvCell... sourceCells) {
        this.episodeOfCare.setCareManager(practitionerReference);

        auditValue("careManager.reference", sourceCells);
    }

    public void setRegistrationStartDate(Date date, CsvCell... sourceCells) {
        Period period = this.episodeOfCare.getPeriod();
        if (period == null) {
            period = new Period();
            this.episodeOfCare.setPeriod(period);
        }
        period.setStart(date);

        calculateActiveState();

        auditValue("period.start", sourceCells);
    }

    public void setRegistrationEndDate(Date date, CsvCell... sourceCells) {
        Period period = this.episodeOfCare.getPeriod();
        if (period == null) {
            period = new Period();
            this.episodeOfCare.setPeriod(period);
        }
        period.setEnd(date);

        calculateActiveState();

        auditValue("period.end", sourceCells);
    }

    /**
     * when we set the period, we call this to derive the active status from it
     */
    private void calculateActiveState() {

        boolean active = PeriodHelper.isActive(this.episodeOfCare.getPeriod());

        if (active) {
            this.episodeOfCare.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
        } else {
            this.episodeOfCare.setStatus(EpisodeOfCare.EpisodeOfCareStatus.FINISHED);
        }
    }

    public void setConfidential(boolean isConfidential, CsvCell... sourceCells) {
        createOrUpdateIsConfidentialExtension(isConfidential, sourceCells);
    }

    public void setRegistrationType(RegistrationType registrationType, CsvCell... sourceCells) {
        Coding coding = CodingHelper.createCoding(registrationType);
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.episodeOfCare, FhirExtensionUri.PATIENT_REGISTRATION_TYPE, coding);

        auditCodingExtension(extension, sourceCells);
    }


}
