package org.endeavourhealth.transform.tpp.csv.schema.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRRepeatTemplate extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRRepeatTemplate.class);

    public SRRepeatTemplate(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_90)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_87)
                || version.equals(TppCsvToFhirTransformer.VERSION_89)) {
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
                    "DateMedicationTemplateStart",
                    "DateMedicationTemplateEnd",
                    "DateMedicationTemplateReview",
                    "MedicationDosage",
                    "MedicationQuantity",
                    "MaxIssues",
                    "CourseLengthPerIssue",
                    "DrugStatus",
                    "IDReferralIn",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_91)
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
                    "IDMultiLexProduct",
                    "IDMultiLexPack",
                    "IDMultiLexDMD",
                    "NameOfMedication",
                    "DateMedicationTemplateStart",
                    "DateMedicationTemplateEnd",
                    "DateMedicationTemplateReview",
                    "MedicationDosage",
                    "MedicationQuantity",
                    "MaxIssues",
                    "CourseLengthPerIssue",
                    "DrugStatus",
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
                    "IDMultiLexProduct",
                    "IDMultiLexPack",
                    "IDMultiLexDMD",
                    "NameOfMedication",
                    "DateMedicationTemplateStart",
                    "DateMedicationTemplateEnd",
                    "DateMedicationTemplateReview",
                    "MedicationDosage",
                    "MedicationQuantity",
                    "MaxIssues",
                    "CourseLengthPerIssue",
                    "DrugStatus",
                    "IDReferralIn",
                    "IDEvent",
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

    public CsvCell getDateMedicationTemplateStart() {
        return super.getCell("DateMedicationTemplateStart");
    }

    public CsvCell getDateMedicationTemplateEnd() {
        return super.getCell("DateMedicationTemplateEnd");
    }

    public CsvCell getDateMedicationTemplateReview() {
        return super.getCell("DateMedicationTemplateReview");
    }

    public CsvCell getMedicationDosage() {
        return super.getCell("MedicationDosage");
    }

    public CsvCell getMedicationQuantity() {
        return super.getCell("MedicationQuantity");
    }

    public CsvCell getMaxIssues() {
        return super.getCell("MaxIssues");
    }

    public CsvCell getCourseLengthPerIssue() {
        return super.getCell("CourseLengthPerIssue");
    }

    public CsvCell getDrugStatus() {
        return super.getCell("DrugStatus");
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
