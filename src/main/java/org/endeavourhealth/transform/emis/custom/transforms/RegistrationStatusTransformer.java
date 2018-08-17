package org.endeavourhealth.transform.emis.custom.transforms;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.custom.helpers.EmisCustomCsvHelper;
import org.endeavourhealth.transform.emis.custom.schema.RegistrationStatus;

public class RegistrationStatusTransformer {

    public static void transform(AbstractCsvParser parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCustomCsvHelper csvHelper) throws Exception {

        while (parser.nextRecord()) {

            try {
                processRecord((RegistrationStatus) parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(RegistrationStatus parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCustomCsvHelper csvHelper) throws Exception {

        CsvCell patientGuidCell = parser.getPatientGuid();
        CsvCell dateCell = parser.getDate();
        CsvCell regStatusCell = parser.getRegistrationStatus();
        CsvCell regTypeCell = parser.getRegistrationType();
        CsvCell processingOrderCell = parser.getProcessingOrder();

        csvHelper.cacheRegStatus(patientGuidCell, regStatusCell, regTypeCell, dateCell, processingOrderCell);
    }

}
