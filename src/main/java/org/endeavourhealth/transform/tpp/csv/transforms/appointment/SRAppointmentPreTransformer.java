package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.appointment.SRAppointment;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRAppointmentPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRAppointmentPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRAppointment.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    processRecord((SRAppointment) parser, csvHelper);

                } catch (Exception ex) {
                    //since we're a pre-transformer, don't log and continue if we get a record-level
                    //exception, as failure to process anything in here must be treated as a critical failure
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }
        }

        //essential that all the callables are complete before we continue
        csvHelper.waitUntilThreadPoolIsEmpty();
    }


    private static void processRecord(SRAppointment parser, TppCsvHelper csvHelper) throws Exception {

        //if deleted, skip ths record
        CsvCell removedCell = parser.getRemovedData();
        if (removedCell != null //null check as the column isn't always present
                && removedCell.getIntAsBoolean()) {
            return;
        }

        //pre-cache rota clinician, because the SRRota transformer needs this information
        CsvCell rotaIdCell = parser.getIDRota();
        CsvCell profileIdCell = parser.getIDProfileClinician();
        if (profileIdCell.isEmpty()
                || profileIdCell.getString().equals("-1")) {
            throw new Exception("Unexpected IDProfileClinician cell value: " + profileIdCell);
        }

        csvHelper.getRotaDateAndStaffCache().cacheRotaProfile(rotaIdCell, profileIdCell);
    }
}
