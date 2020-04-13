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

/**
 * ensures that all UserInRoleGUIDs in the file are cached in the admin helper so we know
 * which staff are actually referenced by clinical data
 */
public class IssueRecordPreTransformer2 {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        IssueRecord parser = (IssueRecord)parsers.get(IssueRecord.class);
        while (parser != null && parser.nextRecord()) {

            try {
                if (csvHelper.shouldProcessRecord(parser)) {
                    CsvCell clinicianCell = parser.getClinicianUserInRoleGuid();
                    csvHelper.getAdminHelper().addRequiredUserInRole(clinicianCell);

                    CsvCell enteredCell = parser.getEnteredByUserInRoleGuid();
                    csvHelper.getAdminHelper().addRequiredUserInRole(enteredCell);
                }

            } catch (Exception ex) {
                //because this is a pre-transformer to cache data, throw any exception so we don't continue
                throw new TransformException(parser.getCurrentState().toString(), ex);
            }
        }
    }
}
