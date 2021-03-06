package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRVariableDoseCDMedication extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRVariableDoseCDMedication.class);

    public SRVariableDoseCDMedication(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "IDMultiLexProduct",
                "IDMultiLexPack",
                "IDMultiLexDMD",
                "NameOfMedication",
                "DateMedicationStart",
                "DateMedicationEnd",
                "MedicationCourseLength",
                "SupervisedDose",
                "MedicationEndReason",
                "IDProfileMedicationEndedBy",
                "IDReferralIn",
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

    public CsvCell getIDMultiLexProduct() {
        return super.getCell("IDMultiLexProduct");
    }

    public CsvCell getIDMultiLexPack() {
        return super.getCell("IDMultiLexPack");
    }

    public CsvCell getIDMultiLexDMD() {
        return super.getCell("IDMultiLexDMD");
    }

    public CsvCell getNameOfMedication() {
        return super.getCell("NameOfMedication");
    }

    public CsvCell getDateMedicationStart() {
        return super.getCell("DateMedicationStart");
    }

    public CsvCell getDateMedicationEnd() {
        return super.getCell("DateMedicationEnd");
    }

    public CsvCell getMedicationCourseLength() {
        return super.getCell("MedicationCourseLength");
    }

    public CsvCell getSupervisedDose() {
        return super.getCell("SupervisedDose");
    }

    public CsvCell getMedicationEndReason() {
        return super.getCell("MedicationEndReason");
    }

    public CsvCell getIDProfileMedicationEndedBy() {
        return super.getCell("IDProfileMedicationEndedBy");
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


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
