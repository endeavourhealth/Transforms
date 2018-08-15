package org.endeavourhealth.transform.tpp.csv.schema.staff;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRStaffMember extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRStaffMember.class);

    public SRStaffMember(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                || version.equals(TppCsvToFhirTransformer.VERSION_88)) {
            return new String[]{
                    "RowIdentifier",
                    "StaffName",
                    "StaffUserName",
                    "NationalIdType",
                    "IDNational",
                    "IDSmartCard",
                    "Obsolete"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_89)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_3)) {
            return new String[]{
                    "RowIdentifier",
                    "StaffName",
                    "StaffUserName",
                    "NationalIdType",
                    "IDNational",
                    "IDSmartCard",
                    "Obsolete",
                    "RemovedData"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "StaffName",
                    "StaffUserName",
                    "NationalIdType",
                    "IDNational",
                    "IDSmartCard",
                    "Obsolete",
                    "RemovedData"
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getStaffName() {
        return super.getCell("StaffName");
    }

    public CsvCell getStaffUserName() {
        return super.getCell("StaffUserName");
    }

    public CsvCell getNationalIdType() {
        return super.getCell("NationalIdType");
    }

    public CsvCell getIDNational() {
        return super.getCell("IDNational");
    }

    public CsvCell getIDSmartCard() {
        return super.getCell("IDSmartCard");
    }

    public CsvCell getObsolete() {
        return super.getCell("Obsolete");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }

    @Override
    protected String getFileTypeDescription() {
        return "TPP staff member file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
