package org.endeavourhealth.transform.homertonrf.transforms;

import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.homerton.transforms.HomertonBasisTransformer;
import org.endeavourhealth.transform.homertonrf.HomertonRfCodeableConceptHelper;
import org.endeavourhealth.transform.homertonrf.HomertonRfCsvHelper;
import org.endeavourhealth.transform.homertonrf.schema.PersonLanguage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PersonLanguageTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PersonLanguageTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonRfCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            if (parser != null) {
                while (parser.nextRecord()) {
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
                                             HomertonRfCsvHelper csvHelper) throws Exception {

        //if there is a sequence number and it is not 1 then return out as we only currently support one language
        CsvCell languageSeqCell = parser.getLanguageSequence();
        if (!languageSeqCell.isEmpty()) {
            if (!languageSeqCell.getString().equalsIgnoreCase("1")) {
                return;
            }
        }

        CsvCell personEmpiCell = parser.getPersonEmpiId();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().getPatientBuilder(personEmpiCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        CodeableConceptBuilder.removeExistingCodeableConcept(patientBuilder, CodeableConceptBuilder.Tag.Patient_Language, null);

        CsvCell languageCodeCell = parser.getLanguageCernerCode();
        HomertonRfCodeableConceptHelper.applyCodeDescTxt(languageCodeCell, CodeValueSet.LANGUAGE, patientBuilder, CodeableConceptBuilder.Tag.Patient_Language, csvHelper);

        //no need to save the resource now, as all patient resources are saved at the end of the Patient transform section
        //here we simply return the patient builder to the cache
        csvHelper.getPatientCache().returnPatientBuilder(personEmpiCell, patientBuilder);
    }
}
