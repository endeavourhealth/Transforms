package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.List;

public class EpisodeOfCareBuilder extends ResourceBuilderBase implements HasIdentifierI, HasContainedListI {

    private EpisodeOfCare episodeOfCare = null;

    public EpisodeOfCareBuilder() {
        this(null);
    }

    public EpisodeOfCareBuilder(EpisodeOfCare episodeOfCare) {
        this(episodeOfCare, null);
    }

    public EpisodeOfCareBuilder(EpisodeOfCare episodeOfCare, ResourceFieldMappingAudit audit) {
        super(audit);

        this.episodeOfCare = episodeOfCare;
        if (this.episodeOfCare == null) {
            this.episodeOfCare = new EpisodeOfCare();
            this.episodeOfCare.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_EPISODE_OF_CARE));
        }
    }

    public void setPatient(Reference referenceValue, CsvCell... sourceCells) {
        this.episodeOfCare.setPatient(referenceValue);

        auditValue("patient.reference", sourceCells);
    }

    public Reference getPatient() {
        return this.episodeOfCare.getPatient();
    }

    @Override
    public DomainResource getResource() {
        return episodeOfCare;
    }

    @Override
    public String getContainedListExtensionUrl() {
        return FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_STATUS;
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

        //active state is only based on end date, so don't pass in our source cells
        calculateActiveState();

        auditValue("period.start", sourceCells);
    }

    public Date getRegistrationStartDate() {
        if (!this.episodeOfCare.hasPeriod()) {
            return null;
        }
        return this.episodeOfCare.getPeriod().getStart();
    }

    public void setRegistrationEndDate(Date date, CsvCell... sourceCells) {
        Period period = this.episodeOfCare.getPeriod();
        if (period == null) {
            period = new Period();
            this.episodeOfCare.setPeriod(period);
        }
        period.setEnd(date);

        calculateActiveState(sourceCells);

        auditValue("period.end", sourceCells);
    }

    /*public void setRegistrationEndDateNoStatusUpdate(Date date, CsvCell... sourceCells) {
        Period period = this.episodeOfCare.getPeriod();
        if (period == null) {
            period = new Period();
            this.episodeOfCare.setPeriod(period);
        }
        period.setEnd(date);

        auditValue("period.end", sourceCells);
    }*/

    public Date getRegistrationEndDate() {
        if (!this.episodeOfCare.hasPeriod()) {
            return null;
        }
        return this.episodeOfCare.getPeriod().getEnd();
    }


    /**
     * when we set the period, we call this to derive the active status from it
     */
    private void calculateActiveState(CsvCell... sourceCells) {

        boolean active = PeriodHelper.isActive(this.episodeOfCare.getPeriod());

        if (active) {
            //this.episodeOfCare.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
            setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE, sourceCells);
        } else {
            //this.episodeOfCare.setStatus(EpisodeOfCare.EpisodeOfCareStatus.FINISHED);
            setStatus(EpisodeOfCare.EpisodeOfCareStatus.FINISHED, sourceCells);
        }

        //auditValue("status", sourceCells);
    }

    public void setStatus(EpisodeOfCare.EpisodeOfCareStatus status, CsvCell... sourceCells) {
        this.episodeOfCare.setStatus(status);

        auditValue("status", sourceCells);
    }

    public void setConfidential(boolean isConfidential, CsvCell... sourceCells) {
        createOrUpdateIsConfidentialExtension(isConfidential, sourceCells);
    }

    public void setRegistrationType(RegistrationType registrationType, CsvCell... sourceCells) {
        if (registrationType == null) {
            ExtensionConverter.removeExtension(this.episodeOfCare, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);

        } else {
            Coding coding = CodingHelper.createCoding(registrationType);
            Extension extension = ExtensionConverter.createOrUpdateExtension(this.episodeOfCare, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE, coding);

            auditCodingExtension(extension, sourceCells);
        }
    }

    //reg status is now stored in a contained list
    /*public void setMedicalRecordStatus(String registrationStatus, CsvCell... sourceCells) {
        StringType stringType = new StringType(registrationStatus);
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.episodeOfCare, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_STATUS, stringType);

        auditCodingExtension(extension, sourceCells);
    }*/

    public void setOutcome(String episodeOutcome, CsvCell... sourceCells) {
        StringType stringType = new StringType(episodeOutcome);
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.episodeOfCare, FhirExtensionUri.EPISODE_OF_CARE_OUTCOME, stringType);

        auditCodingExtension(extension, sourceCells);
    }

    public void setPriority(String episodePriority, CsvCell... sourceCells) {
        StringType stringType = new StringType(episodePriority);
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.episodeOfCare, FhirExtensionUri.EPISODE_OF_CARE_PRIORITY, stringType);

        auditCodingExtension(extension, sourceCells);
    }

    public void setPCCArrival(Date pccArrival, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateDateTimeExtension(this.episodeOfCare, FhirExtensionUri.EPISODE_OF_CARE_PCCARRIVAL, pccArrival);

        auditDateTimeExtension(extension, sourceCells);
    }

    @Override
    public Identifier addIdentifier() {
        return this.episodeOfCare.addIdentifier();
    }

    @Override
    public String getIdentifierJsonPrefix(Identifier identifier) {
        int index = this.episodeOfCare.getIdentifier().indexOf(identifier);
        return "identifier[" + index + "]";
    }

    @Override
    public List<Identifier> getIdentifiers() {
        return this.episodeOfCare.getIdentifier();
    }

    @Override
    public void removeIdentifier(Identifier identifier) {
        this.episodeOfCare.getIdentifier().remove(identifier);
    }


    public boolean hasRegistrationType() {
        return ExtensionConverter.hasExtension(this.episodeOfCare, FhirExtensionUri.EPISODE_OF_CARE_REGISTRATION_TYPE);
    }

    public EpisodeOfCare.EpisodeOfCareStatus getStatus() {
        if (episodeOfCare.hasStatus()) {
            return episodeOfCare.getStatus();
        } else {
            return null;
        }
    }
}
