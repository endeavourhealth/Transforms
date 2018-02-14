package org.endeavourhealth.transform.common.resourceBuilders;


import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class EncounterBuilder extends ResourceBuilderBase
                                implements HasCodeableConceptI,
        HasContainedListI {

    private Encounter encounter = null;

    public EncounterBuilder() {
        this(null);
    }

    public EncounterBuilder(Encounter encounter) {
        this.encounter = encounter;
        if (this.encounter == null) {
            this.encounter = new Encounter();
            this.encounter.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_ENCOUNTER));
        }
    }

    @Override
    public DomainResource getResource() {
        return encounter;
    }

    @Override
    public String getContainedListExtensionUrl() {
        return FhirExtensionUri.ENCOUNTER_COMPONENTS;
    }

    public void setPatient(Reference referenceValue, CsvCell... sourceCells) {
        this.encounter.setPatient(referenceValue);

        auditValue("patient.reference", sourceCells);
    }

    public void addEpisodeOfCare(Reference episodeReference, CsvCell... sourceCells) {
        this.encounter.addEpisodeOfCare(episodeReference);

        int index = this.encounter.getEpisodeOfCare().size()-1;
        auditValue("episodeOfCare[" + index + "].reference", sourceCells);
    }

    public void setStatus(Encounter.EncounterState status, CsvCell... sourceCells) {
        this.encounter.setStatus(status);

        auditValue("status", sourceCells);
    }

    public void setAppointment(Reference appointmentReference, CsvCell... sourceCells) {
        this.encounter.setAppointment(appointmentReference);

        auditValue("appointment", sourceCells);
    }

    public void setIncomplete(CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateBooleanExtension(this.encounter, FhirExtensionUri.ENCOUNTER_INCOMPLETE, true);

        auditBooleanExtension(extension, sourceCells);
    }

    public void setConfidential(boolean isConfidential, CsvCell... sourceCells) {
        createOrUpdateIsConfidentialExtension(isConfidential, sourceCells);
    }

    public void setRecordedBy(Reference practitionerReference, CsvCell... sourceCells) {
        createOrUpdateRecordedByExtension(practitionerReference, sourceCells);
    }

    public void setRecordedDate(Date recordedDate , CsvCell... sourceCells) {
        createOrUpdateRecordedDateExtension(recordedDate, sourceCells);
    }

    public void setServiceProvider(Reference organisationReference, CsvCell... sourceCells) {
        this.encounter.setServiceProvider(organisationReference);

        auditValue("serviceProvider.reference", sourceCells);
    }

    public void addParticipant(Reference practitionerReference, EncounterParticipantType type, CsvCell... sourceCells) {
        Encounter.EncounterParticipantComponent fhirParticipant = this.encounter.addParticipant();
        fhirParticipant.addType(CodeableConceptHelper.createCodeableConcept(type));
        fhirParticipant.setIndividual(practitionerReference);

        int index = this.encounter.getParticipant().size()-1;
        auditValue("participant[" + index + "].individual.reference", sourceCells);
    }

    public void setPeriodStart(DateTimeType startDateTime, CsvCell... sourceCells) {
        Period period = null;
        if (this.encounter.hasPeriod()) {
            period = this.encounter.getPeriod();
        } else {
            period = new Period();
            this.encounter.setPeriod(period);
        }
        period.setStartElement(startDateTime);

        auditValue("period.start", sourceCells);
    }

    public void setEncounterSourceTerm(String term, CsvCell... sourceCells) {
        getOrCreateCodeableConcept(null).setText(term);

        auditValue(getCodeableConceptJsonPath(null) + ".text", sourceCells);
    }


    public Encounter getEncounter() {
        return encounter;
    }


    @Override
    public CodeableConcept getOrCreateCodeableConcept(String tag) {
        Extension extension = ExtensionConverter.findOrCreateExtension(this.encounter, FhirExtensionUri.ENCOUNTER_SOURCE);
        CodeableConcept codeableConcept = (CodeableConcept)extension.getValue();
        if (codeableConcept == null) {
            codeableConcept = new CodeableConcept();
            extension.setValue(codeableConcept);
        }
        return codeableConcept;
    }

    @Override
    public String getCodeableConceptJsonPath(String tag) {
        Extension extension = ExtensionConverter.findExtension(this.encounter, FhirExtensionUri.ENCOUNTER_SOURCE);
        if (extension == null) {
            throw new IllegalArgumentException("Can't call getCodeableConceptJsonPath() before calling getOrCreateCodeableConcept()");
        }

        int index = this.encounter.getExtension().indexOf(extension);
        return "extension[" + index + "].valueCodeableConcept";
    }
}
