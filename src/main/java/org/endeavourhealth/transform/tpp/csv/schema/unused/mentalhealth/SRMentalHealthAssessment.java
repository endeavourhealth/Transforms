package org.endeavourhealth.transform.tpp.csv.schema.unused.mentalhealth;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRMentalHealthAssessment extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRMentalHealthAssessment.class);

    public SRMentalHealthAssessment(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "RowIdentifier",
                "IDOrganisationVisibleTo",
                "ReviewDate",
                "SettledAccommodationIndicator",
                "AccommodationStatus",
                "EmploymentStatus",
                "WeeklyHoursWorked",
                "SexualAbuseQuestionAsked",
                "DateCreation",
                "IDProfileCreatedBy",
                "IDCPA",
                "IDEvent",
                "IDPatient",
                "IDOrganisation"


        };

    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getReviewDate() {
        return super.getCell("ReviewDate");
    }

    public CsvCell getSettledAccommodationIndicator() {
        return super.getCell("SettledAccommodationIndicator");
    }

    public CsvCell getAccommodationStatus() {
        return super.getCell("AccommodationStatus");
    }

    public CsvCell getEmploymentStatus() {
        return super.getCell("EmploymentStatus");
    }

    public CsvCell getWeeklyHoursWorked() {
        return super.getCell("WeeklyHoursWorked");
    }

    public CsvCell getSexualAbuseQuestionAsked() {
        return super.getCell("SexualAbuseQuestionAsked");
    }

    public CsvCell getDateCreation() {
        return super.getCell("DateCreation");
    }

    public CsvCell getIDProfileCreatedBy() {
        return super.getCell("IDProfileCreatedBy");
    }

    public CsvCell getIDCPA() {
        return super.getCell("IDCPA");
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


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
