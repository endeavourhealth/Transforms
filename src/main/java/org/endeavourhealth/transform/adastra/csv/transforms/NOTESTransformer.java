package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.NOTES;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.FlagBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.hl7.fhir.instance.model.Flag;
import org.hl7.fhir.instance.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class NOTESTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(NOTESTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 AdastraCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(NOTES.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((NOTES) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createResource(NOTES parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        CsvCell caseId = parser.getCaseId();
        //CsvCell patientId = parser.getPatientId();  //current bug in extract misses last char from PatientRef
        CsvCell patientId = csvHelper.findCasePatient(caseId.getString());
        CsvCell reviewDateTime = parser.getReviewDateTime();

        //create a unique id for flag.
        String flagId = reviewDateTime.getString()
                + ":" + caseId.getString()
                + ":" + patientId.getString();

        FlagBuilder flagBuilder = new FlagBuilder();
        flagBuilder.setId(flagId, caseId, patientId);
        flagBuilder.setSubject(csvHelper.createPatientReference(patientId), patientId);

        if (!reviewDateTime.isEmpty()) {
            flagBuilder.setStartDate(reviewDateTime.getDate(), reviewDateTime);
        }

        CsvCell active = parser.getActive();
        if (!active.isEmpty()) {
            if (active.getBoolean()) {
                flagBuilder.setStatus(Flag.FlagStatus.ACTIVE, active);
            } else {
                flagBuilder.setStatus(Flag.FlagStatus.INACTIVE, active);
            }
        }

        CsvCell noteText = parser.getNoteText();
        if (!noteText.isEmpty()) {
            flagBuilder.setCode(noteText.getString(), noteText);
        }

        //store the Case Number as a secondary identifier
        CsvCell caseNo = csvHelper.findCaseCaseNo(caseId.getString());
        if (!caseNo.isEmpty()) {

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(flagBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ADASTRA_CASENO);
            identifierBuilder.setValue(caseNo.getString(), caseNo);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), flagBuilder);
    }
}
