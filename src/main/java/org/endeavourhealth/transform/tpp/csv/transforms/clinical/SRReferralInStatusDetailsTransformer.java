package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.referral.SRReferralInStatusDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRReferralInStatusDetailsTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRReferralInStatusDetailsTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRReferralInStatusDetails.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    processRecord((SRReferralInStatusDetails) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(SRReferralInStatusDetails parser, FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper) {

        //if a status record is deleted, it will be replaced with a new status, and will
        //overwrite the current status. So we don't need to worry about the old status.
        CsvCell deleteData = parser.getRemovedData();
        if (deleteData != null && deleteData.getIntAsBoolean()) {
            return;
        }

        CsvCell referralIdCell = parser.getIDReferralIn();
        CsvCell referralStatusCell = parser.getStatusOfReferralIn();
        CsvCell dateCell = parser.getDateEvent();

        csvHelper.getReferralStatusCache().cacheReferralStatus(referralIdCell, dateCell, referralStatusCell);
    }
}
