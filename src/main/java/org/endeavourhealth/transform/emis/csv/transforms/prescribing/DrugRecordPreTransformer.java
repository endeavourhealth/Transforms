package org.endeavourhealth.transform.emis.csv.transforms.prescribing;

import org.endeavourhealth.core.exceptions.CodeNotFoundException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.prescribing.DrugRecord;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;

public class DrugRecordPreTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        AbstractCsvParser parser = parsers.get(DrugRecord.class);
        while (parser != null && parser.nextRecord()) {

            try {
                processLine((DrugRecord) parser, fhirResourceFiler, csvHelper);
            } catch (CodeNotFoundException ex) {
                String errorRecClsName = Thread.currentThread().getStackTrace()[1].getClassName();
                csvHelper.logErrorRecord(ex, ((DrugRecord) parser).getPatientGuid(), ((DrugRecord) parser).getDrugRecordGuid(), errorRecClsName);
            }
        }
    }


    private static void processLine(DrugRecord parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper) throws Exception {

        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            return;
        }

        //if this record is linked to a problem, store this relationship in the helper
        CsvCell problemGuid = parser.getProblemObservationGuid();
        if (!problemGuid.isEmpty()) {

            CsvCell drugRecordGuid = parser.getDrugRecordGuid();
            CsvCell patientGuid = parser.getPatientGuid();

            csvHelper.cacheProblemRelationship(problemGuid,
                    patientGuid,
                    drugRecordGuid,
                    ResourceType.MedicationStatement);
        }
    }

}