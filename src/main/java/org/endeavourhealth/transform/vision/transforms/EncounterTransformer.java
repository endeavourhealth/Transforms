package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.helpers.VisionDateTimeHelper;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class EncounterTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EncounterTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(org.endeavourhealth.transform.vision.schema.Encounter.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((org.endeavourhealth.transform.vision.schema.Encounter) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(org.endeavourhealth.transform.vision.schema.Encounter parser,
                                        FhirResourceFiler fhirResourceFiler,
                                        VisionCsvHelper csvHelper) throws Exception {

        EncounterBuilder encounterBuilder = new EncounterBuilder();

        CsvCell consultationIdCell = parser.getConsultationID();
        CsvCell patientIdCell = parser.getPatientID();

        VisionCsvHelper.setUniqueId(encounterBuilder, patientIdCell, consultationIdCell);

        encounterBuilder.setPatient(csvHelper.createPatientReference(patientIdCell));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell actionCell = parser.getAction();
        if (actionCell.getString().equalsIgnoreCase("D")) {
            encounterBuilder.setDeletedAudit(actionCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
            return;
        }

        //we can receive data for already-deleted patients, in which case ignore
        //this is normally done automatically by FhirResourceFiler, but the createEpisodeReference(..)
        //function fails if we call it for a deleted patient (see SD-65)
        if (fhirResourceFiler.isPatientDeleted(patientIdCell.getString())) {
            TransformWarnings.log(LOG, fhirResourceFiler, "Ignoring save of Vision Encounter {} because patient resource is deleted", consultationIdCell);
            return;
        }

        //link the consultation to our episode of care
        Reference episodeReference = csvHelper.createEpisodeReference(patientIdCell);
        encounterBuilder.setEpisodeOfCare(episodeReference);
        //we have no status field in the source data, but will only receive completed encounters, so we can infer this
        encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

        CsvCell clinicianId = parser.getClinicianUserID();
        //note null check because this column doesn't exist on test data
        if (clinicianId != null && !clinicianId.isEmpty()) {
            String cleanUserID = csvHelper.cleanUserId(clinicianId.getString());
            Reference practitionerReference = csvHelper.createPractitionerReference(cleanUserID);
            encounterBuilder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER, clinicianId);
        }

        //NOTE: there is no recorded date for Vision encounter extracts, just the effective date
        CsvCell dateCell = parser.getEffectiveDate();
        CsvCell timeCell = parser.getEffectiveTime();
        DateTimeType dateTimeType = VisionDateTimeHelper.getDateTime(dateCell, timeCell);
        if (dateTimeType != null) {
            encounterBuilder.setPeriodStart(dateTimeType, dateCell, timeCell);
        }

        CsvCell organisationID = parser.getOrganisationID();
        if (!organisationID.isEmpty()) {
            encounterBuilder.setServiceProvider(csvHelper.createOrganisationReference(organisationID.getString()), organisationID);
        }

        CsvCell sessionTypeCode = parser.getConsultationSessionTypeCode();
        if (sessionTypeCode != null) {
            String term = convertSessionTypeCode(sessionTypeCode.getString());
            if (!Strings.isNullOrEmpty(term)) {
                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
                codeableConceptBuilder.setText(term);
            }
        }

        CsvCell locationTypeCode = parser.getConsultationLocationTypeCode();
        if (!locationTypeCode.isEmpty()) {
            String term = convertLocationTypeCode(locationTypeCode.getString());
            if (!Strings.isNullOrEmpty(term)) {
                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Location_Type);
                codeableConceptBuilder.setText(term);
            }
        }

        ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);

        //carry over linked items from any previous instance of this encounter.
        ReferenceList previousReferences = csvHelper.findConsultationPreviousLinkedResources(encounterBuilder.getResourceId());
        containedListBuilder.addReferences(previousReferences);

        //apply any new linked items from this extract. Encounter links set-up in Journal pre-transformer
        ReferenceList newLinkedResources = csvHelper.getAndRemoveNewConsultationRelationships(encounterBuilder.getResourceId());
        containedListBuilder.addReferences(newLinkedResources);

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
    }

    private static String convertSessionTypeCode(String sessionTypeCode) throws Exception {
        switch (sessionTypeCode) {
            case "G": return "General consultations";
            case "V": return "Visit";
            case "N": return "Night Visit";
            case "A": return "Ante natal";
            case "H": return "Health promotion";
            case "C": return "Child health surveillance";
            case "3": return "Three year check";
            case "7": return "75+ check";
            case "O": return "Other";
            default: throw new TransformException("Unexpected Encounter Session Type Code: [" + sessionTypeCode + "]");
        }
    }

    private static String convertLocationTypeCode(String locationTypeCode) throws Exception {
        switch (locationTypeCode) {
            case "S": return "Surgery";
            case "C": return "Clinic";
            case "A": return "A+E";
            case "H": return "Hospital";
            case "O": return "Other";
            default: throw new TransformException("Unexpected Encounter Location Type Code: [" + locationTypeCode + "]");
        }
    }



}