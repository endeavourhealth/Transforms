package org.endeavourhealth.transform.nwlextracts.transforms;

import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.enterprise.ObservationCodeHelper;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.endeavourhealth.transform.nwlextracts.helpers.NwlExtractsHelper;
import org.endeavourhealth.transform.nwlextracts.schema.PPATI;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Observation;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ObservationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ObservationTransformer.class);


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        NwlExtractsHelper nwlextractsHelper = new NwlExtractsHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(),
                fhirResourceFiler.getExchangeId(), null);
        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {

                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
                    continue;
                }

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                createObservation((PPATI) parser, fhirResourceFiler, csvHelper,nwlextractsHelper,sourceSystem);
            }
        }
    }

    public static void createObservation(PPATI parser, FhirResourceFiler fhirResourceFiler, NwlExtractsCsvHelper csvHelper,NwlExtractsHelper nwlextractsHelper, String sourceSystem) throws Exception {

        CsvCell millenniumPersonIdCell = parser.getMillenniumPersonId();
        CsvCell mrnCell = parser.getLocalPatientId();

        ArrayList<String> patientIdentifiers = new ArrayList<>();
        patientIdentifiers.add(0,millenniumPersonIdCell.getString());
        patientIdentifiers.add(1,mrnCell.getString());

        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(millenniumPersonIdCell,sourceSystem);
        if (patientBuilder == null) {
            return;
        }

        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            patientBuilder.setDeletedAudit(activeCell);
            csvHelper.getPatientCache().deletePatient(patientBuilder, millenniumPersonIdCell, fhirResourceFiler, parser.getCurrentState());
            return;
        }

        //Observation for the Language
        CsvCell languageCell = parser.getFirstLanguageCode();
        String language = null;
        if (!NwlExtractsCsvHelper.isEmptyOrIsZero(languageCell)) {
            CodeableConceptBuilder.removeExistingCodeableConcept(patientBuilder, CodeableConceptBuilder.Tag.Patient_Language, null);
            language = languageCell.getString();

        }

            ObservationBuilder observationBuilderForLanguage = null;
            Observation existingObservationforLanguage = null;

            for (String patient : patientIdentifiers) {
                existingObservationforLanguage = (Observation) nwlextractsHelper.retrieveResource(patient + "Language", ResourceType.Observation);
                if (existingObservationforLanguage != null) {
                    break;
                }
            }
            if (existingObservationforLanguage != null) {
                observationBuilderForLanguage = new ObservationBuilder(existingObservationforLanguage);
            } else {
                observationBuilderForLanguage = new ObservationBuilder();
                NwlExtractsHelper.setUniqueId(observationBuilderForLanguage, NwlExtractsHelper.getPatientGUID(patientIdentifiers) + "Language", null);
            }

            if( language!=null && (!language.equals("0"))) {
                CodeableConcept ccValue = observationBuilderForLanguage.createNewCodeableConcept(CodeableConceptBuilder.Tag.Observation_Main_Code, true);
                observationBuilderForLanguage.removeCodeableConcept(CodeableConceptBuilder.Tag.Observation_Main_Code, ccValue);
                CodeableConcept ccValueForLanguage = observationBuilderForLanguage.createNewCodeableConcept(CodeableConceptBuilder.Tag.Observation_Main_Code, true);

                MapResponse propertyResponse = NwlExtractsHelper.retrieveIMLookupMap(sourceSystem, "language", null);
                MapResponse valueResponse = NwlExtractsHelper.retrieveIMLookupMap(sourceSystem, "language", language);

                if (valueResponse.getWasCreated()) {
                    NwlExtractsHelper.raiseMissingAlert(language, "language", sourceSystem, nwlextractsHelper.getExchangeId().toString(), NwlExtractsHelper.getPatientGUID(patientIdentifiers), nwlextractsHelper);
                }

                String codingSystem = ObservationCodeHelper.mapImSchemeToCodingSystem(valueResponse.getConcept().getScheme());

                ccValueForLanguage.setText(propertyResponse.getConcept().getCode());
                ccValueForLanguage.addCoding().setCode(valueResponse.getConcept().getCode()).setSystem(codingSystem).setDisplay(language);
            }

            if (existingObservationforLanguage != null) {
                Reference patientReference = NwlExtractsHelper.createPatientReference(patientBuilder.getResourceId());
                observationBuilderForLanguage.setPatient(patientReference);
                nwlextractsHelper.MapRemainingSourceIdsWithEDSId(NwlExtractsHelper.getRemainingSourceids(patientIdentifiers, "Language"), observationBuilderForLanguage.getResource().getId(), ResourceType.Observation);
                fhirResourceFiler.savePatientResource(null, false, observationBuilderForLanguage);
            } else {
                observationBuilderForLanguage.setPatient(NwlExtractsHelper.createReference(ResourceType.Patient, patientBuilder.getIdentifiers().get(0).getValue()));
                UUID EdsId = nwlextractsHelper.mapLocalIdToEDSId(NwlExtractsHelper.getPatientGUID(patientIdentifiers) + "Language", ResourceType.Observation);
                nwlextractsHelper.MapRemainingSourceIdsWithEDSId(NwlExtractsHelper.getRemainingSourceids(patientIdentifiers, "Language"), String.valueOf(EdsId), ResourceType.Observation);

                fhirResourceFiler.savePatientResource(null, true, observationBuilderForLanguage);
        }

        //Observation for the Religion
        CsvCell religionCell = parser.getReligionCode();
        String religion=null;
        if (!NwlExtractsCsvHelper.isEmptyOrIsZero(religionCell)) {
            religion=religionCell.getString();
        }
        ObservationBuilder observationBuilderForReligion = null;
        Observation existingObservationForReligion = null;

        for (String patient : patientIdentifiers) {
            existingObservationForReligion = (Observation) nwlextractsHelper.retrieveResource(patient + "Religion", ResourceType.Observation);
            if (existingObservationForReligion != null) {
                break;
            }
        }

        if (existingObservationForReligion != null) {
            observationBuilderForReligion = new ObservationBuilder(existingObservationForReligion);
        } else {
            observationBuilderForReligion = new ObservationBuilder();
            NwlExtractsHelper.setUniqueId(observationBuilderForReligion, NwlExtractsHelper.getPatientGUID(patientIdentifiers) + "Religion", null);
        }

        if (religion != null && (!religion.equals("0"))) {
            CodeableConcept ccValue = observationBuilderForReligion.createNewCodeableConcept(CodeableConceptBuilder.Tag.Observation_Main_Code, true);
            observationBuilderForReligion.removeCodeableConcept(CodeableConceptBuilder.Tag.Observation_Main_Code, ccValue);
            CodeableConcept ccValueForReligion = observationBuilderForReligion.createNewCodeableConcept(CodeableConceptBuilder.Tag.Observation_Main_Code, true);

            MapResponse propertyResponse = NwlExtractsHelper.retrieveIMLookupMap(sourceSystem, "religion", null);
            MapResponse valueResponse = NwlExtractsHelper.retrieveIMLookupMap(sourceSystem, "religion", religion);

            if (valueResponse.getWasCreated()) {
                NwlExtractsHelper.raiseMissingAlert(religion, "religion", sourceSystem, nwlextractsHelper.getExchangeId().toString(), NwlExtractsHelper.getPatientGUID(patientIdentifiers), nwlextractsHelper);
            }

            String codingSystem = ObservationCodeHelper.mapImSchemeToCodingSystem(valueResponse.getConcept().getScheme());

            ccValueForReligion.setText(propertyResponse.getConcept().getCode());
            ccValueForReligion.addCoding().setCode(valueResponse.getConcept().getCode()).setSystem(codingSystem).setDisplay(religion);
        }

        if (existingObservationForReligion != null) {
            Reference patientReference = NwlExtractsHelper.createPatientReference(patientBuilder.getResourceId());
            observationBuilderForReligion.setPatient(patientReference);
            nwlextractsHelper.MapRemainingSourceIdsWithEDSId(NwlExtractsHelper.getRemainingSourceids(patientIdentifiers,"Religion"),observationBuilderForReligion.getResource().getId(),ResourceType.Observation);
            fhirResourceFiler.savePatientResource(null, false, observationBuilderForReligion);
        } else {
            observationBuilderForReligion.setPatient(NwlExtractsHelper.createReference(ResourceType.Patient, patientBuilder.getIdentifiers().get(0).getValue()));
            UUID EdsId = nwlextractsHelper.mapLocalIdToEDSId(NwlExtractsHelper.getPatientGUID(patientIdentifiers)+"Religion", ResourceType.Observation);
            nwlextractsHelper.MapRemainingSourceIdsWithEDSId(NwlExtractsHelper.getRemainingSourceids(patientIdentifiers,"Religion"),String.valueOf(EdsId),ResourceType.Observation);
            fhirResourceFiler.savePatientResource(null, true, observationBuilderForReligion);
        }

    }
}