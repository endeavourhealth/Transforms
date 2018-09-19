package org.endeavourhealth.transform.tpp.csv.schema.referral;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRReferralOut extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRReferralOut.class);

    public SRReferralOut(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_90)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "TypeOfReferral",
                    "Reason",
                    "IDProfileReferrer",
                    "ServiceOffered",
                    "ReReferral",
                    "Urgency",
                    "PrimaryDiagnosis",
                    "SNOMEDPrimaryDiagnosis",
                    "RecipientID",
                    "RecipientIDType",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "M101140ReasonForOutOfAreaReferralAdultAcuteMentalHealth",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };
        } else  if (version.equals(TppCsvToFhirTransformer.VERSION_91)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "TypeOfReferral",
                    "Reason",
                    "IDProfileReferrer",
                    "ServiceOffered",
                    "ReReferral",
                    "Urgency",
                    "PrimaryDiagnosis",
                    "SNOMEDPrimaryDiagnosis",
                    "RecipientID",
                    "RecipientIDType",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "M101140ReasonForOutOfAreaReferralAdultAcuteMentalHealth",
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
                    "TypeOfReferral",
                    "Reason",
                    "IDProfileReferrer",
                    "ServiceOffered",
                    "ReReferral",
                    "Urgency",
                    "PrimaryDiagnosis",
                    "RecipientID",
                    "RecipientIDType",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "M101140ReasonForOutOfAreaReferralAdultAcuteMentalHealth",
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
                    "TypeOfReferral",
                    "Reason",
                    "IDProfileReferrer",
                    "ServiceOffered",
                    "ReReferral",
                    "Urgency",
                    "PrimaryDiagnosis",
                    "RecipientID",
                    "RecipientIDType",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "M101140ReasonForOutOfAreaReferralAdultAcuteMentalHealth",
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
                    "TypeOfReferral",
                    "Reason",
                    "IDProfileReferrer",
                    "ServiceOffered",
                    "ReReferral",
                    "Urgency",
                    "PrimaryDiagnosis",
                    "RecipientID",
                    "RecipientIDType",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "M101140ReasonForOutOfAreaReferralAdultAcuteMentalHealth",
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

    public CsvCell getTypeOfReferral() {
        return super.getCell("TypeOfReferral");
    }

    public CsvCell getReason() {
        return super.getCell("Reason");
    }

    public CsvCell getIDProfileReferrer() {
        return super.getCell("IDProfileReferrer");
    }

    public CsvCell getServiceOffered() {
        return super.getCell("ServiceOffered");
    }

    public CsvCell getReReferral() {
        return super.getCell("ReReferral");
    }

    public CsvCell getUrgency() {
        return super.getCell("Urgency");
    }

    public CsvCell getPrimaryDiagnosis() {
        return super.getCell("PrimaryDiagnosis");
    }

    public CsvCell getSNOMEDPrimaryDiagnosis() {
        return super.getCell("SNOMEDPrimaryDiagnosis");
    }

    public CsvCell getRecipientID() {
        return super.getCell("RecipientID");
    }

    public CsvCell getRecipientIDType() {
        return super.getCell("RecipientIDType");
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

    public CsvCell getM101140ReasonForOutOfAreaReferralAdultAcuteMentalHealth() {
        return super.getCell("M101140ReasonForOutOfAreaReferralAdultAcuteMentalHealth");
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
