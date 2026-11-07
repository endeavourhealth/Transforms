package org.endeavourhealth.transform.tpp.csv.schema.referral;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRReferralInStatusDetails extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRReferralInStatusDetails.class);

    public SRReferralInStatusDetails(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_87)
                || version.equals(TppCsvToFhirTransformer.VERSION_89)
                || version.equals(TppCsvToFhirTransformer.VERSION_90)
                || version.equals(TppCsvToFhirTransformer.VERSION_93)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "StatusOfReferralIn",
                    "IDReferralIn",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_88)
                || version.equals(TppCsvToFhirTransformer.VERSION_91)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "StatusOfReferralIn",
                    "IDReferralIn",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };

        } else if (version.equals(TppCsvToFhirTransformer.VERSION_A)) {
           return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "StatusOfReferralIn",
                    "IDReferralIn",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "DateEarliestClinicallyAppropriate",  //new field
                    "DateLatestClinicallyAppropriate"     //new field
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_B)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "StatusOfReferralIn",
                    "IDReferralIn",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "DateEarliestClinicallyAppropriate",  //new field
                    "DateLatestClinicallyAppropriate",    //new field
                    "RemovedData"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_C)) {
            //SD-1018
            return new String[] {
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "StatusOfReferralIn",
                    "IDReferralIn",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt"
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
                    "StatusOfReferralIn",
                    "IDReferralIn",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
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

    public CsvCell getStatusOfReferralIn() {
        return super.getCell("StatusOfReferralIn");
    }

    public CsvCell getIDReferralIn() {
        return super.getCell("IDReferralIn");
    }

    public CsvCell getIDEvent() {
        return super.getCell("IDEvent");
    }

    public CsvCell getIDPatient() {
        return super.getCell("IDPatient");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }

    public CsvCell getStatusOfReferralOut() {
        return super.getCell("StatusOfReferralOut");
    }

    public CsvCell getIDReferralOut() {
        return super.getCell("IDReferralOut");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
