package org.endeavourhealth.transform.tpp.csv.schema.staff;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRStaffMemberProfile extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberProfile.class);

    public SRStaffMemberProfile(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_2)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_3)
                || version.equals(TppCsvToFhirTransformer.VERSION_91)
                || version.equals(TppCsvToFhirTransformer.VERSION_88)
                || version.equals(TppCsvToFhirTransformer.VERSION_92)) {
            return new String[]{
                    "RowIdentifier",
                    "DateProfileCreated",
                    "IdProfileCreatedBy",
                    "IDStaffMemberProfileRole",
                    "StaffRole",
                    "DateEmploymentStart",
                    "DateEmploymentEnd",
                    "PPAID",
                    "GPLocalCode",
                    "IDStaffMember",
                    "IDOrganisation",
                    "GmpID"

            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_89)
                || version.equals(TppCsvToFhirTransformer.VERSION_90)
                || version.equals(TppCsvToFhirTransformer.VERSION_87)) {
            return new String[]{
                    "RowIdentifier",
                    "DateProfileCreated",
                    "IdProfileCreatedBy",
                    "IDStaffMemberProfileRole",
                    "StaffRole",
                    "DateEmploymentStart",
                    "DateEmploymentEnd",
                    "PPAID",
                    "GPLocalCode",
                    "IDStaffMember",
                    "IDOrganisation",
                    "GmpID",
                    "RemovedData"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "DateProfileCreated",
                    "IdProfileCreatedBy",
                    "IDStaffMemberProfileRole",
                    "StaffRole",
                    "DateEmploymentStart",
                    "DateEmploymentEnd",
                    "PPAID",
                    "GPLocalCode",
                    "IDStaffMember",
                    "IDOrganisation",
                    "GmpID",
                    "RemovedData"
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getDateProfileCreated() {
        return super.getCell("DateProfileCreated");
    }

    public CsvCell getIdProfileCreatedBy() {
        return super.getCell("IdProfileCreatedBy");
    }

    public CsvCell getIDStaffMemberProfileRole() {
        return super.getCell("IDStaffMemberProfileRole");
    }

    public CsvCell getStaffRole() {
        return super.getCell("StaffRole");
    }

    public CsvCell getDateEmploymentStart() {
        return super.getCell("DateEmploymentStart");
    }

    public CsvCell getDateEmploymentEnd() {
        return super.getCell("DateEmploymentEnd");
    }

    public CsvCell getPPAID() {
        return super.getCell("PPAID");
    }

    public CsvCell getGPLocalCode() {
        return super.getCell("GPLocalCode");
    }

    public CsvCell getIDStaffMember() {
        return super.getCell("IDStaffMember");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }

    public CsvCell getGmpID() {
        return super.getCell("GmpID");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
