package org.endeavourhealth.transform.common.resourceBuilders;


import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.List;

public class EncounterBuilder extends ResourceBuilderBase
                                implements HasCodeableConceptI,
                                        HasContainedListI, HasIdentifierI{

    public static final String TAG_SPECIALTY = "CodeableConceptSpecialty";
    public static final String TAG_TREATMENT_FUNCTION = "CodeableConceptTreatmentFunction";
    public static final String TAG_SOURCE = "CodeableConceptSource";
    public static final String TAG_ENCOUNTER_ADMISSION_TYPE = "CodeableConceptEncounterAdmissionType";

    private Encounter encounter = null;

    public EncounterBuilder() {
        this(null);
    }

    public EncounterBuilder(Encounter encounter) {
        this.encounter = encounter;
        if (this.encounter == null) {
            this.encounter = new Encounter();
            this.encounter.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_ENCOUNTER));
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

    // Maintain status history
    public void setStatus(Encounter.EncounterState status, Date startPeriod, Date endPeriod, CsvCell... sourceCells) {
        Encounter.EncounterState currentStatus = this.encounter.getStatus();

        if (currentStatus != null) {
            // If current status found move to history
            if (startPeriod == null) {
                if (this.encounter.getStatusHistory().size() > 0) {
                    Encounter.EncounterStatusHistoryComponent lastHistEntry = this.encounter.getStatusHistory().get(this.encounter.getStatusHistory().size() - 1);
                    startPeriod = lastHistEntry.getPeriod().getEnd();
                } else {
                    startPeriod = this.encounter.getPeriod().getStart();
                }
            }

            if (endPeriod == null) {
                endPeriod = new Date();
            }

            Period period = new Period();
            period.setStart(startPeriod);
            period.setEnd(endPeriod);

            //Encounter.EncounterStatusHistoryComponent eshc = new Encounter.EncounterStatusHistoryComponent(currentStatus, period);

            Encounter.EncounterStatusHistoryComponent eshc = new Encounter.EncounterStatusHistoryComponent()
                    .setPeriod(period)
                    .setStatus(currentStatus);

            this.encounter.getStatusHistory().add(eshc);
        }
        setStatus(status, sourceCells);
    }

    public void setAppointment(Reference appointmentReference, CsvCell... sourceCells) {
        this.encounter.setAppointment(appointmentReference);

        auditValue("appointment", sourceCells);
    }

    public void setIncomplete(boolean isIncomplete, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateBooleanExtension(this.encounter, FhirExtensionUri.ENCOUNTER_INCOMPLETE, isIncomplete);

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

    private Period getOrCreatePeriod() {
        Period period = this.encounter.getPeriod();
        if (period == null) {
            period = new Period();
            this.encounter.setPeriod(period);
        }
        return period;
    }

    public void setPeriodStart(DateTimeType startDateTime, CsvCell... sourceCells) {
        getOrCreatePeriod().setStartElement(startDateTime);

        auditValue("period.start", sourceCells);
    }
    public void setPeriodStart(Date startDateTime, CsvCell... sourceCells) {
        setPeriodStart(new DateTimeType(startDateTime), sourceCells);
    }

    public void setPeriodEnd(DateTimeType endDateTime, CsvCell... sourceCells) {
        getOrCreatePeriod().setStartElement(endDateTime);

        auditValue("period.end", sourceCells);
    }
    public void setPeriodEnd(Date endDateTime, CsvCell... sourceCells) {
        setPeriodEnd(new DateTimeType(endDateTime), sourceCells);
    }

    /*public void setEncounterSourceTerm(String term, CsvCell... sourceCells) {
        getOrCreateCodeableConcept(null).setText(term);

        auditValue(getCodeableConceptJsonPath(null) + ".text", sourceCells);
    }*/

    /*public void addIdentifier(Identifier identifier, CsvCell... sourceCells) {
        this.encounter.addIdentifier(identifier);

        int index = this.encounter.getIdentifier().size()-1;
        auditValue("identifier[" + index + "].value", sourceCells);
    }*/

    public void addReason(CodeableConcept reason, CsvCell... sourceCells) {
        this.encounter.addReason(reason);

        int index = this.encounter.getReason().size()-1;
        auditValue("reason[" + index + "].value", sourceCells);
    }

    public void setClass(Encounter.EncounterClass encounterClass, CsvCell... sourceCells) {
        this.encounter.setClass_(encounterClass);

        auditValue("class", sourceCells);
    }

    /*public void addExtension(String typeDesc, CodeableConcept fhirCodeableConcept, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createExtension(typeDesc, fhirCodeableConcept);

        auditStringExtension(extension, sourceCells);
    }*/

    public void addLocation(Reference referenceValue, CsvCell... sourceCells) {
        this.encounter.addLocation().setLocation(referenceValue);

        int index = this.encounter.getLocation().size()-1;
        auditValue("location[" + index + "].location", sourceCells);
    }

    @Override
    public CodeableConcept createNewCodeableConcept(String tag) {
        if (tag.equals(TAG_SOURCE)) {
            Extension extension = ExtensionConverter.findOrCreateExtension(this.encounter, FhirExtensionUri.ENCOUNTER_SOURCE);
            CodeableConcept codeableConcept = new CodeableConcept();
            extension.setValue(codeableConcept);
            return codeableConcept;

        } else if (tag.equals(TAG_SPECIALTY)) {
            Extension extension = ExtensionConverter.findOrCreateExtension(this.encounter, FhirExtensionUri.ENCOUNTER_SPECIALTY);
            CodeableConcept codeableConcept = new CodeableConcept();
            extension.setValue(codeableConcept);
            return codeableConcept;

        } else if (tag.equals(TAG_TREATMENT_FUNCTION)) {
            Extension extension = ExtensionConverter.findOrCreateExtension(this.encounter, FhirExtensionUri.ENCOUNTER_TREATMENT_FUNCTION);
            CodeableConcept codeableConcept = new CodeableConcept();
            extension.setValue(codeableConcept);
            return codeableConcept;

        } else if (tag.equals(TAG_ENCOUNTER_ADMISSION_TYPE)) {
            Extension extension = ExtensionConverter.findOrCreateExtension(this.encounter, FhirExtensionUri.ENCOUNTER_ADMISSION_TYPE);
            CodeableConcept codeableConcept = new CodeableConcept();
            extension.setValue(codeableConcept);
            return codeableConcept;

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }

    }

    @Override
    public String getCodeableConceptJsonPath(String tag, CodeableConcept codeableConcept) {
        if (tag.equals(TAG_SOURCE)) {
            Extension extension = ExtensionConverter.findExtension(this.encounter, FhirExtensionUri.ENCOUNTER_SOURCE);
            int index = this.encounter.getExtension().indexOf(extension);
            return "extension[" + index + "].valueCodeableConcept";

        } else if (tag.equals(TAG_SPECIALTY)) {
            Extension extension = ExtensionConverter.findExtension(this.encounter, FhirExtensionUri.ENCOUNTER_SPECIALTY);
            int index = this.encounter.getExtension().indexOf(extension);
            return "extension[" + index + "].valueCodeableConcept";

        } else if (tag.equals(TAG_TREATMENT_FUNCTION)) {
            Extension extension = ExtensionConverter.findExtension(this.encounter, FhirExtensionUri.ENCOUNTER_TREATMENT_FUNCTION);
            int index = this.encounter.getExtension().indexOf(extension);
            return "extension[" + index + "].valueCodeableConcept";

        } else if (tag.equals(TAG_ENCOUNTER_ADMISSION_TYPE)) {
            Extension extension = ExtensionConverter.findExtension(this.encounter, FhirExtensionUri.ENCOUNTER_ADMISSION_TYPE);
            int index = this.encounter.getExtension().indexOf(extension);
            return "extension[" + index + "].valueCodeableConcept";


        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public void removeCodeableConcepts(String tag) {
        if (tag.equals(TAG_SOURCE)) {
            ExtensionConverter.removeExtension(this.encounter, FhirExtensionUri.ENCOUNTER_SOURCE);

        } else if (tag.equals(TAG_SPECIALTY)) {
            ExtensionConverter.removeExtension(this.encounter, FhirExtensionUri.ENCOUNTER_SPECIALTY);

        } else if (tag.equals(TAG_TREATMENT_FUNCTION)) {
            ExtensionConverter.removeExtension(this.encounter, FhirExtensionUri.ENCOUNTER_TREATMENT_FUNCTION);

        } else if (tag.equals(TAG_ENCOUNTER_ADMISSION_TYPE)) {
            ExtensionConverter.removeExtension(this.encounter, FhirExtensionUri.ENCOUNTER_ADMISSION_TYPE);

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }

    }

    @Override
    public Identifier addIdentifier() {
        return this.encounter.addIdentifier();
    }

    @Override
    public String getIdentifierJsonPrefix(Identifier identifier) {
        int index = this.encounter.getIdentifier().indexOf(identifier);
        return "identifier[" + index + "]";
    }

    @Override
    public List<Identifier> getIdentifiers() {
        return this.encounter.getIdentifier();
    }

    @Override
    public void removeIdentifier(Identifier identifier) {
        this.encounter.getIdentifier().remove(identifier);
    }
}
