package org.endeavourhealth.transform.tpp.csv.schema.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SREvent extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SREvent.class);

    public SREvent(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
       if (version.equals(TppCsvToFhirTransformer.VERSION_87)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "IDStaffMemberProfileRole",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "IDAuthorisedBy",
                    "IDProfileAuthorisedBy",
                    "ContactEventLocation",
                    "ContactMethod",
                    "EventIncomplete",
                    "ClinicalEvent",
                    "IDReferralIn",
                    "IDPatient",
                    "IDOrganisation",
                    "IDTeam",
                    "IDBranch",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "IDStaffMemberProfileRole",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "IDAuthorisedBy",
                    "IDProfileAuthorisedBy",
                    "ContactEventLocation",
                    "ContactMethod",
                    "EventIncomplete",
                    "ClinicalEvent",
                    "IDReferralIn",
                    "IDPatient",
                    "IDOrganisation",
                    "IDTeam",
                    "IDBranch",
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

    public CsvCell getIDStaffMemberProfileRole() {
        return super.getCell("IDStaffMemberProfileRole");
    }

    public CsvCell getTextualEventDoneBy() {
        return super.getCell("TextualEventDoneBy");
    }

    public CsvCell getIDOrganisationDoneAt() {
        return super.getCell("IDOrganisationDoneAt");
    }

    public CsvCell getIDAuthorisedBy() {
        return super.getCell("IDAuthorisedBy");
    }

    public CsvCell getIDProfileAuthorisedBy() {
        return super.getCell("IDProfileAuthorisedBy");
    }

    public CsvCell getContactEventLocation() {
        return super.getCell("ContactEventLocation");
    }

    public CsvCell getContactMethod() {
        return super.getCell("ContactMethod");
    }

    public CsvCell getEventIncomplete() {
        return super.getCell("EventIncomplete");
    }

    public CsvCell getClinicalEvent() {
        return super.getCell("ClinicalEvent");
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

    public CsvCell getIDTeam() {
        return super.getCell("IDTeam");
    }

    public CsvCell getIDBranch() {
        return super.getCell("IDBranch");
    }

    public CsvCell getIDOrganisationRegisteredAt() {
        return super.getCell("IDOrganisationRegisteredAt");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }

   @Override
    protected String getFileTypeDescription() {
        return "TPP Event Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
