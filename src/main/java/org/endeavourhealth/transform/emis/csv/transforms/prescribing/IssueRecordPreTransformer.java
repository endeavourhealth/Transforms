package org.endeavourhealth.transform.emis.csv.transforms.prescribing;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisDateTimeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.IssueRecordIssueDate;
import org.endeavourhealth.transform.emis.csv.schema.prescribing.IssueRecord;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;

public class IssueRecordPreTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        IssueRecord parser = (IssueRecord)parsers.get(IssueRecord.class);
        while (parser != null && parser.nextRecord()) {

            try {
                if (csvHelper.shouldProcessRecord(parser)) {
                    processLine(parser, fhirResourceFiler, csvHelper);
                }

            } catch (Exception ex) {
                //because this is a pre-transformer to cache data, throw any exception so we don't continue
                throw new TransformException(parser.getCurrentState().toString(), ex);
            }
        }
    }


    private static void processLine(IssueRecord parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper) throws Exception {

        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            return;
        }

        CsvCell patientGuid = parser.getPatientGuid();

        //cache the date against the drug record GUID, so we can pick it up when processing the DrugRecord CSV to find the last issue date
        CsvCell drugRecordGuid = parser.getDrugRecordGuid();
        if (!drugRecordGuid.isEmpty()) {

            CsvCell effectiveDate = parser.getEffectiveDate();
            CsvCell effectiveDatePrecision = parser.getEffectiveDatePrecision();
            DateTimeType dateTime = EmisDateTimeHelper.createDateTimeType(effectiveDate, effectiveDatePrecision);

            CsvCell courseDurationCell = parser.getCourseDurationInDays();
            Integer courseDuration = courseDurationCell.getInt();

            csvHelper.cacheNewDrugRecordDate(drugRecordGuid, patientGuid, new IssueRecordIssueDate(dateTime, courseDuration, effectiveDate, effectiveDatePrecision));
        }

        //if this record is linked to a problem, store this relationship in the helper
        CsvCell problemGuid = parser.getProblemObservationGuid();
        if (!problemGuid.isEmpty()) {
            CsvCell issueRecordGuid = parser.getIssueRecordGuid();

            csvHelper.cacheProblemRelationship(problemGuid,
                    patientGuid,
                    issueRecordGuid,
                    ResourceType.MedicationOrder);
        }

    }

}
