package org.endeavourhealth.transform.tpp.csv.schema.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRCode extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRCode.class);

    public SRCode(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }


    @Override
    protected String[] getCsvHeaders(String version) {

        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_2)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "CTV3Code",
                    "CTV3Text",
                    "NumericComparator",
                    "NumericValue",
                    "NumericUnit",
                    "EpisodeType",
                    "IDTemplate",
                    "IDEvent",
                    "IDPatient",
                    "IDReferralIn",
                    "IDAppointment",
                    "IDVisit",
                    "IDOrganisation"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_87)
                || version.equals(TppCsvToFhirTransformer.VERSION_88)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "CTV3Code",
                    "CTV3Text",
                    "NumericComparator",
                    "IsNumeric",
                    "NumericValue",
                    "NumericUnit",
                    "EpisodeType",
                    "IDTemplate",
                    "IDEvent",
                    "IDPatient",
                    "IDReferralIn",
                    "IDAppointment",
                    "IDVisit",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_89)) {

            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "CTV3Code",
                    "CTV3Text",
                    "NumericComparator",
                    "IsNumeric",
                    "NumericValue",
                    "NumericUnit",
                    "EpisodeType",
                    "IDTemplate",
                    "IDEvent",
                    "IDPatient",
                    "IDReferralIn",
                    "IDAppointment",
                    "IDVisit",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "CTV3Code",
                    "CTV3Text",
                    "NumericComparator",
                    "IsNumeric",
                    "NumericValue",
                    "NumericUnit",
                    "EpisodeType",
                    "IDTemplate",
                    "IDEvent",
                    "IDPatient",
                    "IDReferralIn",
                    "IDAppointment",
                    "IDVisit",
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

    public CsvCell getDateEventRecorded() {
        return super.getCell("DateEventRecorded");
    }

    public CsvCell getDateEvent() {
        return super.getCell("DateEvent");
    }

    public CsvCell getIDProfileEnteredBy() {
        return super.getCell("IDProfileEnteredBy");
    }

    public CsvCell getIDDoneBy() {
        return super.getCell("IDDoneBy");
    }

    public CsvCell getTextualEventDoneBy() {
        return super.getCell("TextualEventDoneBy");
    }

    public CsvCell getIDOrganisationDoneAt() {
        return super.getCell("IDOrganisationDoneAt");
    }

    public CsvCell getCTV3Code() {
        return super.getCell("CTV3Code");
    }

    public CsvCell getCTV3Text() {
        return super.getCell("CTV3Text");
    }

    public CsvCell getNumericComparator() {
        return super.getCell("NumericComparator");
    }

    public CsvCell getNumericValue() {
        return super.getCell("NumericValue");
    }

    public CsvCell getIsNumeric() {
        return super.getCell("IsNumeric");
    }

    public CsvCell getNumericUnit() {
        return super.getCell("NumericUnit");
    }

    public CsvCell getEpisodeType() {
        return super.getCell("EpisodeType");
    }

    public CsvCell getIDTemplate() {
        return super.getCell("IDTemplate");
    }

    public CsvCell getIDEvent() {
        return super.getCell("IDEvent");
    }

    public CsvCell getIDPatient() {
        return super.getCell("IDPatient");
    }

    public CsvCell getIDReferralIn() {
        return super.getCell("IDReferralIn");
    }

    public CsvCell getIDAppointment() {
        return super.getCell("IDAppointment");
    }

    public CsvCell getIDVisit() {
        return super.getCell("IDVisit");
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
    protected String getFileTypeDescription() {
        return "TPP Main Clinical Observation file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
