package org.endeavourhealth.transform.emis.csv.transforms.appointment;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.appointment.SessionUser;

import java.util.Map;

public class SessionUserTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        AbstractCsvParser parser = parsers.get(SessionUser.class);

        while (parser != null && parser.nextRecord()) {

            //don't continue if an exception is raised, since follow files depend on this working OK
            try {
                createSessionUserMapping((SessionUser)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                throw new TransformException(parser.getCurrentState().toString(), ex);
            }
        }
    }

    private static void createSessionUserMapping(SessionUser parser,
                                                 FhirResourceFiler fhirResourceFiler,
                                                 EmisCsvHelper csvHelper) throws Exception {

        CsvCell deleted = parser.getdDeleted();
        CsvCell sessionGuid = parser.getSessionGuid();
        CsvCell userGuid = parser.getUserInRoleGuid();
        boolean isDeleted = deleted.getBoolean();

        csvHelper.cacheSessionPractitionerMap(sessionGuid, userGuid, isDeleted);
    }
}

