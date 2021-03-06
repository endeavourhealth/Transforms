package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRStaff extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRStaff.class);

    public SRStaff(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_2)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_3)) {
            return new String[]{
                    "RowIdentifier",
                    "IDStaff",
                    "IDStaffProfile",
                    "StaffName",
                    "StaffRole",
                    "StaffUserName",
                    "DateProfileCreated",
                    "DateEmploymentStart",
                    "DateEmploymentEnd",
                    "NationalIdType",
                    "IDNational",
                    "PPAID",
                    "GPLocalCode",
                    "IDOrganisation"
            };
        } else  if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)) {
            return new String[]{
                    "RowIdentifier",
                    "IDStaff",
                    "IDStaffProfile",
                    "StaffName",
                    "StaffRole",
                    "StaffUserName",
                    "DateProfileCreated",
                    "DateEmploymentStart",
                    "DateEmploymentEnd",
                    "NationalIdType",
                    "IDNational",
                    "PPAID",
                    "GPLocalCode",
                    "IDOrganisation"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDStaff",
                    "IDStaffProfile",
                    "StaffName",
                    "StaffRole",
                    "StaffUserName",
                    "DateProfileCreated",
                    "DateEmploymentStart",
                    "DateEmploymentEnd",
                    "NationalIdType",
                    "IDNational",
                    "PPAID",
                    "GPLocalCode",
                    "IDOrganisation",
                    "RemovedData"
            };
        }
    }


    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDStaff() {
        return super.getCell("IDStaff");
    }

    public CsvCell getIDStaffProfile() {
        return super.getCell("IDStaffProfile");
    }

    public CsvCell getStaffName() {
        return super.getCell("StaffName");
    }

    public CsvCell getStaffRole() {
        return super.getCell("StaffRole");
    }

    public CsvCell getStaffUserName() {
        return super.getCell("StaffUserName");
    }

    public CsvCell getDateProfileCreated() {
        return super.getCell("DateProfileCreated");
    }

    public CsvCell getDateEmploymentStart() {
        return super.getCell("DateEmploymentStart");
    }

    public CsvCell getDateEmploymentEnd() {
        return super.getCell("DateEmploymentEnd");
    }

    public CsvCell getNationalIdType() {
        return super.getCell("NationalIdType");
    }

    public CsvCell getIDNational() {
        return super.getCell("IDNational");
    }

    public CsvCell getPPAID() {
        return super.getCell("PPAID");
    }

    public CsvCell getGPLocalCode() {
        return super.getCell("GPLocalCode");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
