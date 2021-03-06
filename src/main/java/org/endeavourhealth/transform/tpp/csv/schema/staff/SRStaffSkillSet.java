package org.endeavourhealth.transform.tpp.csv.schema.staff;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRStaffSkillSet extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRStaffSkillSet.class);

    public SRStaffSkillSet(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_89)
                || version.equals(TppCsvToFhirTransformer.VERSION_90)
                || version.equals((TppCsvToFhirTransformer.VERSION_87))) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateCreated",
                    "IdProfileCreatedBy",
                    "IDStaffProfile",
                    "DateSkillSetStart",
                    "DateSkillSetEnd",
                    "SkillSetName",
                    "IDOrganisation",
                    "RemovedData"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_88)
                || version.equals(TppCsvToFhirTransformer.VERSION_91)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateCreated",
                    "IdProfileCreatedBy",
                    "IDStaffProfile",
                    "DateSkillSetStart",
                    "DateSkillSetEnd",
                    "SkillSetName",
                    "IDOrganisation"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateCreated",
                    "IdProfileCreatedBy",
                    "IDStaffProfile",
                    "DateSkillSetStart",
                    "DateSkillSetEnd",
                    "SkillSetName",
                    "IDOrganisation"
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getDateCreated() {
        return super.getCell("DateCreated");
    }

    public CsvCell getIdProfileCreatedBy() {
        return super.getCell("IdProfileCreatedBy");
    }

    public CsvCell getIDStaffProfile() {
        return super.getCell("IDStaffProfile");
    }

    public CsvCell getDateSkillSetStart() {
        return super.getCell("DateSkillSetStart");
    }

    public CsvCell getDateSkillSetEnd() {
        return super.getCell("DateSkillSetEnd");
    }

    public CsvCell getSkillSetName() {
        return super.getCell("SkillSetName");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
