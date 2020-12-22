package org.endeavourhealth.transform.emis.csv.transforms.appointment;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.appointment.Session;
import org.endeavourhealth.transform.emis.csv.schema.appointment.SessionUser;
import org.endeavourhealth.transform.emis.csv.schema.prescribing.IssueRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SessionUserPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SessionUserPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        SessionUser parser = (SessionUser)parsers.get(SessionUser.class);
        while (parser != null && parser.nextRecord()) {

            try {
                CsvCell userCell = parser.getUserInRoleGuid();
                csvHelper.getAdminHelper().addRequiredUserInRole(userCell);

            } catch (Exception ex) {
                //because this is a pre-transformer to cache data, throw any exception so we don't continue
                throw new TransformException(parser.getCurrentState().toString(), ex);
            }
        }
    }
}
