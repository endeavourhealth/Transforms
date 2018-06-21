package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class EncounterBuilder extends ResourceBuilderBase
                                implements HasCodeableConceptI,
                                        HasContainedListI, HasIdentifierI{


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

    public void setEpisodeOfCare(Reference episodeReference, CsvCell... sourceCells) {
        if (this.encounter.hasEpisodeOfCare()) {
            this.encounter.getEpisodeOfCare().remove(0);
        }

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
        setStatus(status, startPeriod, endPeriod, false, sourceCells);
    }

    public void setStatus(Encounter.EncounterState status, Date startPeriod, Date endPeriod, boolean removeIfExists, CsvCell... sourceCells) {
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

            if (removeIfExists && this.encounter.hasStatusHistory()) {
                List<Encounter.EncounterStatusHistoryComponent> histList = this.encounter.getStatusHistory();

                for (Iterator<Encounter.EncounterStatusHistoryComponent> iterator = histList.iterator(); iterator.hasNext();) {
                    Encounter.EncounterStatusHistoryComponent encounterStatusHistoryComponent = iterator.next();

                    if (encounterStatusHistoryComponent.getStatus().getDefinition().compareToIgnoreCase(status.getDefinition()) == 0) {
                        if (encounterStatusHistoryComponent.getPeriod().getStart().compareTo(period.getStart()) == 0) {
                            if (encounterStatusHistoryComponent.getPeriod().getEnd().compareTo(period.getEnd()) == 0) {
                                iterator.remove();
                            }
                        }
                    }
                }
            }

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

    public void setPartOf(Reference visitReference, CsvCell... sourceCells) {
        this.encounter.setPartOf(visitReference);

        auditValue("part Of", sourceCells);
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

    public void addParticipant(Reference practitionerReference, EncounterParticipantType type, boolean removeIfExists, CsvCell... sourceCells) {
        if (removeIfExists) {
            List<Encounter.EncounterParticipantComponent> partList = this.encounter.getParticipant();

            for (Iterator<Encounter.EncounterParticipantComponent> iterator = partList.iterator(); iterator.hasNext();) {
                Encounter.EncounterParticipantComponent epc = iterator.next();
                if (epc.getType().get(0).getCoding().get(0).getSystem().compareToIgnoreCase(CodeableConceptHelper.createCodeableConcept(type).getCoding().get(0).getSystem()) == 0) {
                    if (epc.getIndividual().getReference().compareToIgnoreCase(practitionerReference.getReference()) == 0) {
                        iterator.remove();
                    }
                }
            }
        }
        addParticipant(practitionerReference, type, sourceCells);
    }

    public void addParticipant(Reference practitionerReference, EncounterParticipantType type, Period period, CsvCell... sourceCells) {
        Encounter.EncounterParticipantComponent fhirParticipant = this.encounter.addParticipant();
        fhirParticipant.addType(CodeableConceptHelper.createCodeableConcept(type));
        fhirParticipant.setIndividual(practitionerReference);
        fhirParticipant.setPeriod(period);
        int index = this.encounter.getParticipant().size()-1;
        auditValue("participant[" + index + "].individual.reference", sourceCells);
    }

    public void addParticipant(Reference practitionerReference, EncounterParticipantType type, Period period, boolean removeIfExists, CsvCell... sourceCells) {
        if (removeIfExists) {

        }
        addParticipant(practitionerReference, type, period, sourceCells);
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
        getOrCreatePeriod().setEndElement(endDateTime);

        auditValue("period.end", sourceCells);
    }

    public void setPeriodEnd(Date endDateTime, CsvCell... sourceCells) {
        setPeriodEnd(new DateTimeType(endDateTime), sourceCells);
    }

    public Period getPeriod() {
        if (!this.encounter.hasPeriod()) {
            return null;
        }
        return this.encounter.getPeriod();
    }

    public void setDuration(Duration value, CsvCell... sourceCells) {
        this.encounter.setLength(value);

        auditValue("length", sourceCells);
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

    public void addReason(String reason, CsvCell... sourceCells) {

        //ensure we don't end up with the same reason twice
        if (this.encounter.hasReason()) {

            List<CodeableConcept> reasonList = this.encounter.getReason();
            for (Iterator<CodeableConcept> iterator = reasonList.iterator(); iterator.hasNext();) {
                CodeableConcept cc = iterator.next();
                String text = cc.getText();
                if (text.equalsIgnoreCase(reason)) {
                    //already present
                    return;
                }
            }
        }

        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(reason);
        this.encounter.addReason(codeableConcept);

        int index = this.encounter.getReason().size()-1;
        auditValue("reason[" + index + "].text", sourceCells);
    }

    public void addType(String typeDesc, CsvCell... sourceCells) {

        //ensure we don't end up with the same type twice
        if (this.encounter.hasType()) {

            List<CodeableConcept> types = this.encounter.getType();
            for (Iterator<CodeableConcept> iterator = types.iterator(); iterator.hasNext();) {
                CodeableConcept cc = iterator.next();
                String text = cc.getText();
                if (text.equalsIgnoreCase(typeDesc)) {
                    //already present
                    return;
                }
            }
        }

        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(typeDesc);
        this.encounter.addType(codeableConcept);

        int index = this.encounter.getType().size()-1;
        auditValue("type[" + index + "].text", sourceCells);
    }

    public List<CodeableConcept> getReason() {
        return this.encounter.getReason();
    }

    public boolean hasReason() {
        return this.encounter.hasReason();
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

    public void addLocation(Reference referenceValue, boolean removeIfExists, CsvCell... sourceCells) {
        if (removeIfExists && this.encounter.hasLocation()) {
            List<Encounter.EncounterLocationComponent> locationList = this.encounter.getLocation();

            for (Iterator<Encounter.EncounterLocationComponent> iterator = locationList.iterator(); iterator.hasNext();) {
                Encounter.EncounterLocationComponent currELC = iterator.next();
                if (currELC.getLocation().getReference().compareToIgnoreCase(referenceValue.getReference()) == 0) {
                    iterator.remove();
                }
            }
        }
        addLocation(referenceValue, sourceCells);
    }

    public void addLocation(Encounter.EncounterLocationComponent location, CsvCell... sourceCells) {
        this.encounter.addLocation(location);

        int index = this.encounter.getLocation().size()-1;
        auditValue("location[" + index + "].location", sourceCells);
    }

    /*public void addLocation(Encounter.EncounterLocationComponent location, boolean removeIfExists, CsvCell... sourceCells) {
        if (removeIfExists && this.encounter.hasLocation()) {
            List<Encounter.EncounterLocationComponent> locationList = this.encounter.getLocation();

            for (Iterator<Encounter.EncounterLocationComponent> iterator = locationList.iterator(); iterator.hasNext();) {
                Encounter.EncounterLocationComponent currELC = iterator.next();
                if (currELC.getLocation().getReference().compareToIgnoreCase(location.getLocation().getReference()) == 0) {
                    if (currELC.hasPeriod() && location.hasPeriod()) {
                         if (location.getPeriod().getStart().compareTo(currELC.getPeriod().getStart()) == 0) {
                             iterator.remove();
                         }
                    } else {
                        iterator.remove();
                    }
                }
            }
        }
        addLocation(location, sourceCells);
    }*/

    public Reference getPatient() {
        return this.encounter.getPatient();
    }

    public List<Encounter.EncounterLocationComponent> getLocation() {
        return this.encounter.getLocation();
    }

    @Override
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag) {
        if (tag == CodeableConceptBuilder.Tag.Encounter_Source) {
            Extension extension = ExtensionConverter.findOrCreateExtension(this.encounter, FhirExtensionUri.ENCOUNTER_SOURCE);
            CodeableConcept codeableConcept = new CodeableConcept();
            extension.setValue(codeableConcept);
            return codeableConcept;

        } else if (tag == CodeableConceptBuilder.Tag.Encounter_Specialty) {
            Extension extension = ExtensionConverter.findOrCreateExtension(this.encounter, FhirExtensionUri.ENCOUNTER_SPECIALTY);
            CodeableConcept codeableConcept = new CodeableConcept();
            extension.setValue(codeableConcept);
            return codeableConcept;

        } else if (tag == CodeableConceptBuilder.Tag.Encounter_Treatment_Function) {
            Extension extension = ExtensionConverter.findOrCreateExtension(this.encounter, FhirExtensionUri.ENCOUNTER_TREATMENT_FUNCTION);
            CodeableConcept codeableConcept = new CodeableConcept();
            extension.setValue(codeableConcept);
            return codeableConcept;

        /*} else if (tag == CodeableConceptBuilder.Tag.Encounter_Admission_Type) {
            Extension extension = ExtensionConverter.findOrCreateExtension(this.encounter, FhirExtensionUri.ENCOUNTER_ADMISSION_TYPE);
            CodeableConcept codeableConcept = new CodeableConcept();
            extension.setValue(codeableConcept);
            return codeableConcept;*/

        } else if (tag == CodeableConceptBuilder.Tag.Encounter_Location_Type) {
            Extension extension = ExtensionConverter.findOrCreateExtension(this.encounter, FhirExtensionUri.ENCOUNTER_LOCATION_TYPE);
            CodeableConcept codeableConcept = new CodeableConcept();
            extension.setValue(codeableConcept);
            return codeableConcept;

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Encounter_Source) {
            Extension extension = ExtensionConverter.findExtension(this.encounter, FhirExtensionUri.ENCOUNTER_SOURCE);
            int index = this.encounter.getExtension().indexOf(extension);
            return "extension[" + index + "].valueCodeableConcept";

        } else if (tag == CodeableConceptBuilder.Tag.Encounter_Specialty) {
            Extension extension = ExtensionConverter.findExtension(this.encounter, FhirExtensionUri.ENCOUNTER_SPECIALTY);
            int index = this.encounter.getExtension().indexOf(extension);
            return "extension[" + index + "].valueCodeableConcept";

        } else if (tag == CodeableConceptBuilder.Tag.Encounter_Treatment_Function) {
            Extension extension = ExtensionConverter.findExtension(this.encounter, FhirExtensionUri.ENCOUNTER_TREATMENT_FUNCTION);
            int index = this.encounter.getExtension().indexOf(extension);
            return "extension[" + index + "].valueCodeableConcept";

        /*} else if (tag == CodeableConceptBuilder.Tag.Encounter_Admission_Type) {
            Extension extension = ExtensionConverter.findExtension(this.encounter, FhirExtensionUri.ENCOUNTER_ADMISSION_TYPE);
            int index = this.encounter.getExtension().indexOf(extension);
            return "extension[" + index + "].valueCodeableConcept";*/

        } else if (tag == CodeableConceptBuilder.Tag.Encounter_Location_Type) {
            Extension extension = ExtensionConverter.findExtension(this.encounter, FhirExtensionUri.ENCOUNTER_LOCATION_TYPE);
            int index = this.encounter.getExtension().indexOf(extension);
            return "extension[" + index + "].valueCodeableConcept";

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {

        if (tag == CodeableConceptBuilder.Tag.Encounter_Source) {
            ExtensionConverter.removeExtension(this.encounter, FhirExtensionUri.ENCOUNTER_SOURCE);

        } else if (tag == CodeableConceptBuilder.Tag.Encounter_Specialty) {
            ExtensionConverter.removeExtension(this.encounter, FhirExtensionUri.ENCOUNTER_SPECIALTY);

        } else if (tag == CodeableConceptBuilder.Tag.Encounter_Treatment_Function) {
            ExtensionConverter.removeExtension(this.encounter, FhirExtensionUri.ENCOUNTER_TREATMENT_FUNCTION);

        /*} else if (tag == CodeableConceptBuilder.Tag.Encounter_Admission_Type) {
            ExtensionConverter.removeExtension(this.encounter, FhirExtensionUri.ENCOUNTER_ADMISSION_TYPE);*/

        } else if (tag == CodeableConceptBuilder.Tag.Encounter_Location_Type) {
            ExtensionConverter.removeExtension(this.encounter, FhirExtensionUri.ENCOUNTER_LOCATION_TYPE);

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

    public static boolean removeExistingLocation(EncounterBuilder encounterBuilder, String idValue) {
        if (Strings.isNullOrEmpty(idValue)) {
            throw new IllegalArgumentException("Can't remove location without ID");
        }

        List<Encounter.EncounterLocationComponent> matches = new ArrayList<>();

        List<Encounter.EncounterLocationComponent> locations = encounterBuilder.getLocation();
        for (Encounter.EncounterLocationComponent location: locations) {
            //if we match on ID, then remove this name from the parent object
            if (location.hasId()
                    && location.getId().equals(idValue)) {

                matches.add(location);
            }
        }

        if (matches.isEmpty()) {
            return false;

        } else if (matches.size() > 1) {
            throw new IllegalArgumentException("Found " + matches.size() + " names for ID " + idValue);

        } else {
            Encounter.EncounterLocationComponent match = matches.get(0);

            //remove any audits we've created for the Name
            String identifierJsonPrefix = "location[" + (locations.indexOf(match)-1) + "]";
            encounterBuilder.getAuditWrapper().removeAudit(identifierJsonPrefix);

            Encounter encounter = (Encounter)encounterBuilder.getResource();
            encounter.getLocation().remove(match);
            return true;
        }
    }
}
