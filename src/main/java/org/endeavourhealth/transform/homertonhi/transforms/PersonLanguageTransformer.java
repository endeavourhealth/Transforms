package org.endeavourhealth.transform.homertonhi.transforms;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.schema.PersonLanguage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.endeavourhealth.common.fhir.FhirCodeUri.CODE_SYSTEM_NHS_DD;

public class PersonLanguageTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PersonLanguageTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonHiCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            if (parser != null) {
                while (parser.nextRecord()) {

                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }
                    try {
                        transform((PersonLanguage) parser, fhirResourceFiler, csvHelper);
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void transform(PersonLanguage parser,
                                             FhirResourceFiler fhirResourceFiler,
                                             HomertonHiCsvHelper csvHelper) throws Exception {

        //NOTE: we only currently support one language.  The sequence numbers in the data files are always 1
        //so provide no indication of primary / secondary languages anyway.  Deletes not supported.

        CsvCell personEmpiIdCell = parser.getPersonEmpiId();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().getPatientBuilder(personEmpiIdCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        CodeableConceptBuilder.removeExistingCodeableConcept(patientBuilder, CodeableConceptBuilder.Tag.Patient_Language, null);
        CodeableConceptBuilder languageCodeableConceptBuilder
                = new CodeableConceptBuilder(patientBuilder, CodeableConceptBuilder.Tag.Patient_Language);

        CsvCell languageDisplayCell = parser.getLanguageDisplay();
        CsvCell languagePrimaryDisplayCell = parser.getLanguagePrimaryDisplay();
        CsvCell languageCodeCell = parser.getLanguageCode();
        //the languages are NHS_DD coded or just free text
        if (!languageCodeCell.isEmpty()) {

            languageCodeableConceptBuilder.addCoding(CODE_SYSTEM_NHS_DD);
            languageCodeableConceptBuilder.setCodingCode(languageCodeCell.getString(), languageCodeCell);
            languageCodeableConceptBuilder.setCodingDisplay(languageDisplayCell.getString(), languageDisplayCell);
            languageCodeableConceptBuilder.setText(languagePrimaryDisplayCell.getString(), languagePrimaryDisplayCell);

            if (languageCodeCell.getString().equalsIgnoreCase("en")) {
                patientBuilder.setSpeaksEnglish(Boolean.TRUE, languageCodeCell);
            }

        } else {

            languageCodeableConceptBuilder.setText(languagePrimaryDisplayCell.getString(), languagePrimaryDisplayCell);
        }

        //no need to save the resource now, as all patient resources are saved at the end of the Patient transform section
        //here we simply return the patient builder to the cache
        csvHelper.getPatientCache().returnPatientBuilder(personEmpiIdCell, patientBuilder);
    }
}
