package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRPlaceHolderMedication extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRPlaceHolderMedication.class);

    public SRPlaceHolderMedication(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "NameOfMedication",
                "DateValidFrom",
                "DateValidTo",
                "Transcribing",
                "DateMedicationEnd",
                "MedicationEndReason",
                "IDProfileMedicationEndedBy",
                "SourceOfSupply",
                "MedicationApproved",
                "IDMedicationApprovedBy",
                "IDProfileMedicationApprovedBy",
                "IDReferralIn",
                "IDOrganisation",
                "IDEvent",
                "IDPatient"


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

    public CsvCell getNameOfMedication() {
        return super.getCell("NameOfMedication");
    }

    public CsvCell getDateValidFrom() {
        return super.getCell("DateValidFrom");
    }

    public CsvCell getDateValidTo() {
        return super.getCell("DateValidTo");
    }

    public CsvCell getTranscribing() {
        return super.getCell("Transcribing");
    }

    public CsvCell getDateMedicationEnd() {
        return super.getCell("DateMedicationEnd");
    }

    public CsvCell getMedicationEndReason() {
        return super.getCell("MedicationEndReason");
    }

    public CsvCell getIDProfileMedicationEndedBy() {
        return super.getCell("IDProfileMedicationEndedBy");
    }

    public CsvCell getSourceOfSupply() {
        return super.getCell("SourceOfSupply");
    }

    public CsvCell getMedicationApproved() {
        return super.getCell("MedicationApproved");
    }

    public CsvCell getIDMedicationApprovedBy() {
        return super.getCell("IDMedicationApprovedBy");
    }

    public CsvCell getIDProfileMedicationApprovedBy() {
        return super.getCell("IDProfileMedicationApprovedBy");
    }

    public CsvCell getIDReferralIn() {
        return super.getCell("IDReferralIn");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }

    public CsvCell getIDEvent() {
        return super.getCell("IDEvent");
    }

    public CsvCell getIDPatient() {
        return super.getCell("IDPatient");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
