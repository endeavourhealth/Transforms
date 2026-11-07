package org.endeavourhealth.transform.adastra.csv.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.CLINICALCODES;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Observation;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CLINICALCODESTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(CLINICALCODESTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 AdastraCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(CLINICALCODES.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((CLINICALCODES) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(CLINICALCODES parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        CsvCell caseId = parser.getCaseId();
        CsvCell consultationId = parser.getConsultationId();
        CsvCell clinicalCode = parser.getCode();

        String observationId = caseId.getString()
                + ":" + consultationId.getString()
                + ":" + clinicalCode.getString();

        ObservationBuilder observationBuilder = new ObservationBuilder();
        observationBuilder.setId(observationId, caseId, consultationId, clinicalCode);

        CsvCell patientId = csvHelper.findCasePatient(caseId.getString());
        if (!patientId.isEmpty()) {

            observationBuilder.setPatient(csvHelper.createPatientReference(patientId));
        } else {
            TransformWarnings.log(LOG, parser, "No Patient id in record for CaseId: {},  file: {}",
                    caseId.getString(), parser.getFilePath());
            return;
        }

        //status is mandatory, so set the only value we can
        observationBuilder.setStatus(Observation.ObservationStatus.UNKNOWN);

        //find the consultation date to use with code, otherwise null
        CsvCell effectiveDate = csvHelper.findConsultationDateTime(consultationId.getString());
        if (effectiveDate != null) {

            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDateTime());
            observationBuilder.setEffectiveDate(dateTimeType, effectiveDate);
        }

        //v2 userRef - get from consultation transformer
        CsvCell userRefCell = csvHelper.findConsultationUserRef(consultationId.getString());
        if (userRefCell != null) {

            Reference practitionerReference = csvHelper.createPractitionerReference(userRefCell.getString());
            observationBuilder.setClinician(practitionerReference, userRefCell);
        }

        //clinicalCode (previously readCode) can now be either Read2, Snomed or something else
        //SD-884 fixes the setting of Snomed codes against the Read2 scheme now we detect what code type it might be
        if (!clinicalCode.isEmpty()) {

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(observationBuilder, CodeableConceptBuilder.Tag.Observation_Main_Code);

            //work out coding type. Read2 is 5 bytes, Snomed is pure numeric > 5 length
            String code = clinicalCode.getString().trim();
            int codeLength = clinicalCode.getString().length();
            if ((codeLength > 5) && (StringUtils.isNumeric(code))) {

                // Add Snomed coding only
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                codeableConceptBuilder.setCodingCode(code, clinicalCode);
                CsvCell snomedTerm = parser.getTerm();
                codeableConceptBuilder.setCodingDisplay(snomedTerm.getString(), snomedTerm);
                codeableConceptBuilder.setText(snomedTerm.getString(), snomedTerm);

            } else if (codeLength == 5 || code.contains("-")) {

                // add 5 byte Read2 coding or Read2 Synonym with hyphen
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_READ2);
                codeableConceptBuilder.setCodingCode(code, clinicalCode);
                CsvCell readTerm = parser.getTerm();
                codeableConceptBuilder.setCodingDisplay(readTerm.getString(), readTerm);
                codeableConceptBuilder.setText(readTerm.getString(), readTerm);

                // attempt to add Read2 translation to Snomed
                SnomedCode snomedCode = TerminologyService.translateRead2ToSnomed(code);
                if (snomedCode != null) {

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);
                    codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode());
                    codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm());
                }
            } else {

                TransformWarnings.log(LOG, parser, "Code scheme not detected for Adastra ClinicalCode: {}, file: {}",
                        code, parser.getFilePath());
                return;
            }
        }

        // set consultation/encounter reference
        if (!consultationId.isEmpty()) {

            Reference consultationReference = csvHelper.createEncounterReference(consultationId);
            observationBuilder.setEncounter(consultationReference, consultationId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), observationBuilder);
    }
}