package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRStaffMemberProfileRole extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberProfileRole.class);

    public SRStaffMemberProfileRole(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_2)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_3)) {
            return new String[] {
                    "RowIdentifier",
                    "RoleDescription",
                    "IsSpineRole",
                    "DateLastAmended"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_88)) {
            return new String[]{
                    "RowIdentifier",
                    "RoleDescription",
                    "IsSpineRole",
                    "DateLastAmended",
                    "DateDeleted"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_89)
                || version.equals(TppCsvToFhirTransformer.VERSION_87)) {
            return new String[]{
                    "RowIdentifier",
                    "RoleDescription",
                    "IsSpineRole",
                    "DateLastAmended",
                    "DateDeleted",
                    "RemovedData"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "RoleDescription",
                    "IsSpineRole",
                    "DateLastAmended",
                    "DateDeleted",
                    "RemovedData"
            };
        }
    }


    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getRoleDescription() {
        return super.getCell("RoleDescription");
    }

    public CsvCell getIsSpineRole() {
        return super.getCell("IsSpineRole");
    }

    public CsvCell getDateLastAmended() {
        return super.getCell("DateLastAmended");
    }

    public CsvCell getDateDeleted() {
        return super.getCell("DateDeleted");
    }

    @Override
    protected String getFileTypeDescription() {
        return "TPP staff member role reference file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
