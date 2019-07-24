package org.endeavourhealth.transform.tpp.csv.schema.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRChildAtRisk extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRChildAtRisk.class);

    public SRChildAtRisk(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if ( version.equals(TppCsvToFhirTransformer.VERSION_93)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_87)
                || version.equals(TppCsvToFhirTransformer.VERSION_89)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateAdded",
                    "IDProfileEnteredBy",
                    "ChildProtectionPlan",
                    "DateRemoved",
                    "IDProfileRemovedBy",
                    "RemovalReason",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };
        } else  if (version.equals(TppCsvToFhirTransformer.VERSION_91)
                ||version.equals(TppCsvToFhirTransformer.VERSION_90)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateAdded",
                    "IDProfileEnteredBy",
                    "ChildProtectionPlan",
                    "DateRemoved",
                    "IDProfileRemovedBy",
                    "RemovalReason",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt"
            };
        } else {return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateAdded",
                    "IDProfileEnteredBy",
                    "ChildProtectionPlan",
                    "DateRemoved",
                    "IDProfileRemovedBy",
                    "RemovalReason",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt"
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getDateAdded() {
        return super.getCell("DateAdded");
    }

    public CsvCell getIDProfileEnteredBy() {
        return super.getCell("IDProfileEnteredBy");
    }

    public CsvCell getChildProtectionPlan() {
        return super.getCell("ChildProtectionPlan");
    }

    public CsvCell getDateRemoved() {
        return super.getCell("DateRemoved");
    }

    public CsvCell getIDProfileRemovedBy() {
        return super.getCell("IDProfileRemovedBy");
    }

    public CsvCell getRemovalReason() {
        return super.getCell("RemovalReason");
    }

    public CsvCell getIDPatient() {
        return super.getCell("IDPatient");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }

    public CsvCell getIDOrganisationRegisteredAt() {
        return super.getCell("IDOrganisationRegisteredAt");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
