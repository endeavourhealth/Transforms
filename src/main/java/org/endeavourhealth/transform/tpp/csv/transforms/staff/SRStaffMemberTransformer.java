package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMember;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRStaffMemberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRStaffMember.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    processRecord((SRStaffMember) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(SRStaffMember parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell staffMemberId = parser.getRowIdentifier();

        CsvCell fullName = parser.getStaffName();
        CsvCell userName = parser.getStaffUserName();
        CsvCell nationalId = parser.getIDNational();
        CsvCell nationalIdType = parser.getNationalIdType();
        CsvCell smartCardId = parser.getIDSmartCard();
        CsvCell obsolete = parser.getObsolete();

        StaffMemberCacheObj cacheObj = new StaffMemberCacheObj(fullName, userName, nationalId, nationalIdType, smartCardId, obsolete);
        csvHelper.getStaffMemberCache().addStaffMemberObj(staffMemberId, cacheObj);
    }

}
