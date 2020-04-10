package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppMappingRefDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.TppStaffDalI;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMember;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

/**
 * pre-transformer to cache staff member details in memory for a later transform to use
 */
public class SRStaffMemberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        SRStaffMember parser = (SRStaffMember)parsers.get(SRStaffMember.class);

        if (parser != null) {

            //we need to go through the file records to make sure it's audited
            while (parser.nextRecord()) {
                //just spin through it
            }

            //bulk load the file into the DB
            String filePath = parser.getFilePath();
            int fileId = parser.getFileAuditId().intValue();
            Date dataDate = fhirResourceFiler.getDataDate();
            TppStaffDalI dal = DalProvider.factoryTppStaffMemberDal();
            dal.updateStaffMemberLookupTable(filePath, dataDate, fileId);
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    /*private static void processRecord(SRStaffMember parser,
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
    }*/

}
