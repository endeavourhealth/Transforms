package org.endeavourhealth.transform.tpp.csv.schema.unused.careplan;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRCarePlanDetail extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRCarePlanDetail.class);

    public SRCarePlanDetail(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "DateEventRecorded",
                "DateEvent",
                "IDProfileEnteredBy",
                "IDDoneBy",
                "TextualEventDoneBy",
                "IDOrganisationDoneAt",
                "NameOfCarePlanUsed",
                "CarePlanCategory",
                "CarePlanSubCategory",
                "Aim",
                "Outcome",
                "IDReferralIn",
                "IDEvent",
                "IDPatient",
                "IDOrganisation",
                "PlanType",
                "PatientAgreement",
                "AfterCare117"


        };

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

    public CsvCell getNameOfCarePlanUsed() {
        return super.getCell("NameOfCarePlanUsed");
    }

    public CsvCell getCarePlanCategory() {
        return super.getCell("CarePlanCategory");
    }

    public CsvCell getCarePlanSubCategory() {
        return super.getCell("CarePlanSubCategory");
    }

    public CsvCell getAim() {
        return super.getCell("Aim");
    }

    public CsvCell getOutcome() {
        return super.getCell("Outcome");
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

    public CsvCell getPlanType() {
        return super.getCell("PlanType");
    }

    public CsvCell getPatientAgreement() {
        return super.getCell("PatientAgreement");
    }

    public CsvCell getAfterCare117() {
        return super.getCell("AfterCare117");
    }


    //fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {
        return "TPP SRCarePlanDetail Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
