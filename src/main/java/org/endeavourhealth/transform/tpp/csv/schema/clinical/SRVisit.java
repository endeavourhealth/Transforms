package org.endeavourhealth.transform.tpp.csv.schema.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRVisit extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRVisit.class);

    public SRVisit(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_93)
                || version.equals(TppCsvToFhirTransformer.VERSION_90)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_87)
                || version.equals(TppCsvToFhirTransformer.VERSION_89)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "IDProfileEnteredBy",
                    "DateBooked",
                    "DateRequested",
                    "CurrentStatus",
                    "IDProfileRequested",
                    "IDProfileAssigned",
                    "FollowUpDetails",
                    "IDReferralIn",
                    "IDPatient",
                    "IDOrganisation",
                    "Duration",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_91)
                || version.equals(TppCsvToFhirTransformer.VERSION_88)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "IDProfileEnteredBy",
                    "DateBooked",
                    "DateRequested",
                    "CurrentStatus",
                    "IDProfileRequested",
                    "IDProfileAssigned",
                    "FollowUpDetails",
                    "IDReferralIn",
                    "IDPatient",
                    "IDOrganisation",
                    "Duration",
                    "IDOrganisationRegisteredAt"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "IDProfileEnteredBy",
                    "DateBooked",
                    "DateRequested",
                    "CurrentStatus",
                    "IDProfileRequested",
                    "IDProfileAssigned",
                    "FollowUpDetails",
                    "IDReferralIn",
                    "IDPatient",
                    "IDOrganisation",
                    "Duration",
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

    /**
     * datetime that the visit was recorded into SystmOne
     * appears to be identical to "DateBooked"
     */
    public CsvCell getDateEventRecorded() {
        return super.getCell("DateEventRecorded");
    }

    /**
     * staff profile that recorded the visit in SystmOne
     */
    public CsvCell getIDProfileEnteredBy() {
        return super.getCell("IDProfileEnteredBy");
    }

    /**
     * datetime that the visit was recorded into SystmOne
     */
    public CsvCell getDateBooked() {
        return super.getCell("DateBooked");
    }

    /**
     * datetime that the appointment is WANTED (i.e. when it will take place)
     */
    public CsvCell getDateRequested() {
        return super.getCell("DateRequested");
    }

    public CsvCell getCurrentStatus() {
        return super.getCell("CurrentStatus");
    }

    /**
     * the optional profile ID of the practitioner the patient WOULD LIKE to Visit them (rarely used)
     */
    public CsvCell getIDProfileRequested() {
        return super.getCell("IDProfileRequested");
    }

    /**
     * the profile ID of the practitioner who actually is allocated the visit
     */
    public CsvCell getIDProfileAssigned() {
        return super.getCell("IDProfileAssigned");
    }

    /**
     * int, referring to TPP mapping reference file
     * tells us whether the clinician states a follow-up visit is needed or not
     */
    public CsvCell getFollowUpDetails() {
        return super.getCell("FollowUpDetails");
    }

    public CsvCell getIDReferralIn() {
        return super.getCell("IDReferralIn");
    }

    public CsvCell getIDPatient() {
        return super.getCell("IDPatient");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }

    /**
     * the nominal duration allocated for the visit (in mins)
     */
    public CsvCell getDuration() {
        return super.getCell("Duration");
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
