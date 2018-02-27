package org.endeavourhealth.transform.vision.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.openhr.schema.VocDatePart;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class EncounterTransformer {

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(org.endeavourhealth.transform.vision.schema.Encounter.class);
        while (parser.nextRecord()) {

            try {
                createResource((org.endeavourhealth.transform.vision.schema.Encounter)parser, fhirResourceFiler, csvHelper, version);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(org.endeavourhealth.transform.vision.schema.Encounter parser,
                                        FhirResourceFiler fhirResourceFiler,
                                        VisionCsvHelper csvHelper,
                                        String version) throws Exception {

        Encounter fhirEncounter = new Encounter();
        fhirEncounter.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_ENCOUNTER));

        String consultationID = parser.getConsultationID();
        String patientID = parser.getPatientID();

        VisionCsvHelper.setUniqueId(fhirEncounter, patientID, consultationID);

        fhirEncounter.setPatient(csvHelper.createPatientReference(patientID));

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        if (parser.getAction().equalsIgnoreCase("D")) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirEncounter);
            return;
        }

        //link the consultation to our episode of care
        Reference episodeReference = csvHelper.createEpisodeReference(patientID);
        fhirEncounter.addEpisodeOfCare(episodeReference);
        fhirEncounter.setStatus(Encounter.EncounterState.FINISHED);

        String clinicianID = parser.getClinicianUserID();
        if (!Strings.isNullOrEmpty(clinicianID)) {
            Encounter.EncounterParticipantComponent fhirParticipant = fhirEncounter.addParticipant();
            fhirParticipant.addType(CodeableConceptHelper.createCodeableConcept(EncounterParticipantType.PRIMARY_PERFORMER));
            fhirParticipant.setIndividual(csvHelper.createPractitionerReference(clinicianID));
        }

        Date enteredDateTime = parser.getEnteredDateTime();
        if (enteredDateTime != null) {
            fhirEncounter.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.RECORDED_DATE, new DateTimeType(enteredDateTime)));
        }

        Date date = parser.getEffectiveDate();
        String precision = "YMD";
        Period fhirPeriod = createPeriod(date, precision);
        if (fhirPeriod != null) {
            fhirEncounter.setPeriod(fhirPeriod);
        }

        String organisationID = parser.getOrganisationID();
        fhirEncounter.setServiceProvider(csvHelper.createOrganisationReference(organisationID));

        String sessionTypeCode = parser.getConsultationSessionTypeCode();
        if (sessionTypeCode != null) {
            String term = convertSessionTypeCode(sessionTypeCode);
            CodeableConcept fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(term);
            fhirEncounter.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.ENCOUNTER_SOURCE, fhirCodeableConcept));
        }

        String locationTypeCode = parser.getConsultationLocationTypeCode();
        if (locationTypeCode != null) {
            String term = convertLocationTypeCode(locationTypeCode);
            CodeableConcept fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(term);
            fhirEncounter.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.ENCOUNTER_LOCATION_TYPE, fhirCodeableConcept));
        }

        //carry over linked items from any previous instance of this encounter
        List<Reference> previousReferences = VisionCsvHelper.findPreviousLinkedReferences(csvHelper, fhirResourceFiler, fhirEncounter.getId(), ResourceType.Encounter);
        if (previousReferences != null && !previousReferences.isEmpty()) {
            csvHelper.addLinkedItemsToResource(fhirEncounter, previousReferences, FhirExtensionUri.ENCOUNTER_COMPONENTS);
        }

        //apply any linked items from this extract. Encounter links set-up in Journal pre-transformer
        List<String> linkedResources = csvHelper.getAndRemoveConsultationRelationships(consultationID, patientID);
        if (linkedResources != null) {
            List<Reference> references = ReferenceHelper.createReferences(linkedResources);
            csvHelper.addLinkedItemsToResource(fhirEncounter, references, FhirExtensionUri.ENCOUNTER_COMPONENTS);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirEncounter);
    }


    private static Period createPeriod(Date date, String precision) throws Exception {
        if (date == null) {
            return null;
        }

        VocDatePart vocPrecision = VocDatePart.fromValue(precision);
        if (vocPrecision == null) {
            throw new IllegalArgumentException("Unsupported consultation precision [" + precision + "]");
        }

        Period fhirPeriod = new Period();
        switch (vocPrecision) {
            case U:
                return null;
            case Y:
                fhirPeriod.setStartElement(new DateTimeType(date, TemporalPrecisionEnum.YEAR));
                break;
            case YM:
                fhirPeriod.setStartElement(new DateTimeType(date, TemporalPrecisionEnum.MONTH));
                break;
            case YMD:
                fhirPeriod.setStartElement(new DateTimeType(date, TemporalPrecisionEnum.DAY));
                break;
            case YMDT:
                fhirPeriod.setStartElement(new DateTimeType(date, TemporalPrecisionEnum.SECOND));
                break;
            default:
                throw new IllegalArgumentException("Unexpected date precision " + vocPrecision);
        }
        return fhirPeriod;
    }

    private static String convertSessionTypeCode(String sessionTypeCode) {
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
            default: return null;
        }
    }

    private static String convertLocationTypeCode(String locationTypeCode) {
        switch (locationTypeCode) {
            case "S": return "Surgery";
            case "C": return "Clinic";
            case "A": return "A+E";
            case "H": return "Hospital";
            case "O": return "Other";
            default: return null;
        }
    }



}