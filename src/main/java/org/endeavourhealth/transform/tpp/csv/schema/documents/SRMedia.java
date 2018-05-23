package org.endeavourhealth.transform.tpp.csv.schema.documents;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRMedia extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRMedia.class);

    public SRMedia(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_87)
                || version.equals(TppCsvToFhirTransformer.VERSION_89)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DocumentUID",
                    "IDPatient",
                    "IDEvent",
                    "DateEvent",
                    "IDDoneBy",
                    "DateEventRecorded",
                    "IDProfileEnteredBy",
                    "IDOrganisationDoneAt",
                    "FileName",
                    "FileSize",
                    "SenderTitle",
                    "SenderFirstName",
                    "SenderSurname",
                    "SenderOrganisation",
                    "RecipientTitle",
                    "RecipientFirstName",
                    "RecipientSurname",
                    "RecipientOrganisation",
                    "EmailAddress",
                    "Direction",
                    "CommunicationType",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_88)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DocumentUID",
                    "IDPatient",
                    "IDEvent",
                    "DateEvent",
                    "IDDoneBy",
                    "DateEventRecorded",
                    "IDProfileEnteredBy",
                    "IDOrganisationDoneAt",
                    "FileName",
                    "FileSize",
                    "SenderTitle",
                    "SenderFirstName",
                    "SenderSurname",
                    "SenderOrganisation",
                    "RecipientTitle",
                    "RecipientFirstName",
                    "RecipientSurname",
                    "RecipientOrganisation",
                    "EmailAddress",
                    "Direction",
                    "CommunicationType",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DocumentUID",
                    "IDPatient",
                    "IDEvent",
                    "DateEvent",
                    "IDDoneBy",
                    "DateEventRecorded",
                    "IDProfileEnteredBy",
                    "IDOrganisationDoneAt",
                    "FileName",
                    "FileSize",
                    "SenderTitle",
                    "SenderFirstName",
                    "SenderSurname",
                    "SenderOrganisation",
                    "RecipientTitle",
                    "RecipientFirstName",
                    "RecipientSurname",
                    "RecipientOrganisation",
                    "EmailAddress",
                    "Direction",
                    "CommunicationType",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt"
            };
        }
    }


    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getDocumentUID() {
        return super.getCell("DocumentUID");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getIDPatient() {
        return super.getCell("IDPatient");
    }

    public CsvCell getIDEvent() {
        return super.getCell("IDEvent");
    }

    public CsvCell getDateEvent() {
        return super.getCell("DateEvent");
    }

    public CsvCell getIDDoneBy() {
        return super.getCell("IDDoneBy");
    }

    public CsvCell getDateEventRecorded() {
        return super.getCell("DateEventRecorded");
    }

    public CsvCell getIDProfileEnteredBy() {
        return super.getCell("IDProfileEnteredBy");
    }

    public CsvCell getIDOrganisationDoneAt() {
        return super.getCell("IDOrganisationDoneAt");
    }

    public CsvCell getFileName() {
        return super.getCell("FileName");
    }

    public CsvCell getFileSize() {
        return super.getCell("FileSize");
    }

    public CsvCell getSenderTitle() {
        return super.getCell("SenderTitle");
    }

    public CsvCell getSenderFirstName() {
        return super.getCell("SenderFirstName");
    }

    public CsvCell getSenderSurname() {
        return super.getCell("SenderSurname");
    }

    public CsvCell getSenderOrganisation() {
        return super.getCell("SenderOrganisation");
    }

    public CsvCell getRecipientTitle() {
        return super.getCell("RecipientTitle");
    }

    public CsvCell getRecipientFirstName() {
        return super.getCell("RecipientFirstName");
    }

    public CsvCell getRecipientSurname() {
        return super.getCell("RecipientSurname");
    }

    public CsvCell getRecipientOrganisation() {
        return super.getCell("RecipientOrganisation");
    }

    public CsvCell getEmailAddress() {
        return super.getCell("EmailAddress");
    }

    public CsvCell getDirection() {
        return super.getCell("Direction");
    }

    public CsvCell getCommunicationType() {
        return super.getCell("CommunicationType");
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
        return "TPP file containing binary record attachments";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
