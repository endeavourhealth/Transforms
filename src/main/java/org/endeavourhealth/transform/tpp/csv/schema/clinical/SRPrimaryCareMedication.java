package org.endeavourhealth.transform.tpp.csv.schema.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRPrimaryCareMedication extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRPrimaryCareMedication.class);

    public SRPrimaryCareMedication(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                    "DateMedicationStart",
                    "DateMedicationEnd",
                    "MedicationDosage",
                    "MedicationQuantity",
                    "IsSupervised",
                    "SupervisedDose",
                    "UnsupervisedDose",
                    "IsRepeatMedication",
                    "IsOtherMedication",
                    "IsDentalMedication",
                    "IsHospitalMedication",
                    "IDRepeatTemplate",
                    "IDReferralIn",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_88)) {
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
                    "MedicationDosage",
                    "MedicationQuantity",
                    "IsSupervised",
                    "SupervisedDose",
                    "UnsupervisedDose",
                    "IsRepeatMedication",
                    "IsOtherMedication",
                    "IsDentalMedication",
                    "IsHospitalMedication",
                    "IDRepeatTemplate",
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
                    "DateMedicationStart",
                    "DateMedicationEnd",
                    "MedicationDosage",
                    "MedicationQuantity",
                    "IsSupervised",
                    "SupervisedDose",
                    "UnsupervisedDose",
                    "IsRepeatMedication",
                    "IsOtherMedication",
                    "IsDentalMedication",
                    "IsHospitalMedication",
                    "IDRepeatTemplate",
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

    public CsvCell getDateMedicationStart() {
        return super.getCell("DateMedicationStart");
    }

    public CsvCell getDateMedicationEnd() {
        return super.getCell("DateMedicationEnd");
    }

    public CsvCell getMedicationDosage() {
        return super.getCell("MedicationDosage");
    }

    public CsvCell getMedicationQuantity() {
        return super.getCell("MedicationQuantity");
    }

    public CsvCell getIsSupervised() {
        return super.getCell("IsSupervised");
    }

    public CsvCell getSupervisedDose() {
        return super.getCell("SupervisedDose");
    }

    public CsvCell getUnsupervisedDose() {
        return super.getCell("UnsupervisedDose");
    }

    public CsvCell getIsRepeatMedication() {
        return super.getCell("IsRepeatMedication");
    }

    public CsvCell getIsOtherMedication() {
        return super.getCell("IsOtherMedication");
    }

    public CsvCell getIsDentalMedication() {
        return super.getCell("IsDentalMedication");
    }

    public CsvCell getIsHospitalMedication() {
        return super.getCell("IsHospitalMedication");
    }

    public CsvCell getIDRepeatTemplate() {
        return super.getCell("IDRepeatTemplate");
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
    protected String getFileTypeDescription() {
        return "TPP Primary Care Medication Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
