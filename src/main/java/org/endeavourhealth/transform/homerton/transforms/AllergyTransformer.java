package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.AllergyIntoleranceBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.endeavourhealth.transform.homerton.schema.AllergyTable;
import org.hl7.fhir.instance.model.AllergyIntolerance;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AllergyTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(AllergyTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                        createCondition((AllergyTable) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createCondition(AllergyTable parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonCsvHelper csvHelper) throws Exception {

        CsvCell allergyIdCell = parser.getAllergyId();

        AllergyIntoleranceBuilder allergyIntoleranceBuilder = new AllergyIntoleranceBuilder();
        allergyIntoleranceBuilder.setId(allergyIdCell.getString(), allergyIdCell);

        // set patient reference
        CsvCell personIdCell = parser.getPersonId();
        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personIdCell.getString());
        allergyIntoleranceBuilder.setPatient(patientReference, personIdCell);

        //the life cycle status cell tells us to delete
        CsvCell statusCell = parser.getReactionStatus();
        String status = statusCell.getString();
        if (status.equalsIgnoreCase("Canceled")) { //note the US spelling used

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
            return;
        }

        CsvCell allergyDateCell = parser.getAllergyDate();
        if (!allergyDateCell.isEmpty()) {
            DateTimeType onsetDate = new DateTimeType(allergyDateCell.getDateTime());
            allergyIntoleranceBuilder.setOnsetDate(onsetDate, allergyDateCell);
        }

        CsvCell recordedDateCell = parser.getRecordedDate();
        if (!recordedDateCell.isEmpty()) {

            allergyIntoleranceBuilder.setRecordedDate(recordedDateCell.getDateTime(), recordedDateCell);
        }

        // TODO - need personnel data to map
        CsvCell recordedByClinicianID = parser.getRecordedByClinicianID();
        if (!recordedByClinicianID.isEmpty()) {

            Reference reference = csvHelper.createPractitionerReference(recordedByClinicianID.getString());
            allergyIntoleranceBuilder.setRecordedBy(reference, recordedByClinicianID);
        }

        CsvCell encounterIdCell = parser.getEncounterId();
        if (!encounterIdCell.isEmpty()) {

            Reference encounterReference
                    = ReferenceHelper.createReference(ResourceType.Encounter, encounterIdCell.getString());
            allergyIntoleranceBuilder.setEncounter(encounterReference, encounterIdCell);
        }

        // set code to coded problem
        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(allergyIntoleranceBuilder, CodeableConceptBuilder.Tag.Allergy_Intolerance_Main_Code);

        // it's rare, but there are cases where records have a textual term but not vocab or code
        CsvCell allergyCodeCell = parser.getAllergyCode();
        CsvCell vocabCell = parser.getVocabulary();
        CsvCell allergyDescCell = parser.getAllergyDescriptionText();
        if (!vocabCell.isEmpty() && !allergyCodeCell.isEmpty()) {
            String vocab = vocabCell.getString();
            String code = allergyCodeCell.getString();

            if (vocab.equalsIgnoreCase("SNOMED CT")) {
                // the code is a SNOMED description ID, not concept ID, so we need to look up the term differently
                SnomedCode snomedCode = TerminologyService.lookupSnomedConceptForDescriptionId(code);
                if (snomedCode == null) {
                    TransformWarnings.log(LOG, parser, "Failed to lookup Snomed term for allergy descriptionId {}", code);

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_DESCRIPTION_ID, vocabCell);
                    codeableConceptBuilder.setCodingCode(code, allergyCodeCell);

                } else {
                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, vocabCell);
                    codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode(), allergyCodeCell);
                    codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                }
            } else if (vocab.equalsIgnoreCase("Multum Drug")) {

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_MULTUM_DRUG_ID, vocabCell);
                codeableConceptBuilder.setCodingCode(code, allergyCodeCell);
                if (!allergyDescCell.isEmpty()) {
                    codeableConceptBuilder.setCodingDisplay(allergyDescCell.getString(), allergyDescCell);
                }
            } else if (vocab.equalsIgnoreCase("Multum Allergy Category")) {

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_MULTUM_ALLERGY_CATEGORY_ID, vocabCell);
                codeableConceptBuilder.setCodingCode(code, allergyCodeCell);
                if (!allergyDescCell.isEmpty()) {
                    codeableConceptBuilder.setCodingDisplay(allergyDescCell.getString(), allergyDescCell);
                }
            } else {
                throw new TransformException("Unexpected problem VOCAB [" + vocab + "]");
            }
        }

       // set the raw term on the codeable concept text - This is true if the vocab is 'Allergy' for example
        if (!allergyDescCell.isEmpty()) {

            String term = allergyDescCell.getString();
            codeableConceptBuilder.setText(term, allergyDescCell);
        }

        CsvCell severity = parser.getAllergySeverity();
        if (!severity.isEmpty()) {

            if (severity.getString().equalsIgnoreCase("Mild")) {
                allergyIntoleranceBuilder.setSeverity(AllergyIntolerance.AllergyIntoleranceSeverity.MILD, severity);
            } else if (severity.getString().equalsIgnoreCase("Moderate")) {
                allergyIntoleranceBuilder.setSeverity(AllergyIntolerance.AllergyIntoleranceSeverity.MODERATE, severity);
            } if (severity.getString().equalsIgnoreCase("Severe")) {
                allergyIntoleranceBuilder.setSeverity(AllergyIntolerance.AllergyIntoleranceSeverity.SEVERE, severity);
            }
        }


        // save resource
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
    }



}
