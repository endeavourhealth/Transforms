package org.endeavourhealth.transform.tpp.csv.schema.unused.hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRHospitalAdmissionAndDischarge extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRHospitalAdmissionAndDischarge.class);

    public SRHospitalAdmissionAndDischarge(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "DateAdmissionCreated",
                "IdProfileCreatedBy",
                "IntendedManagement",
                "AdmissionConfirmed",
                "AdmissionLetterPrinted",
                "DateAdmitted",
                "IDProfileAdmittedBy",
                "AdmissionNotes",
                "DateCancelled",
                "IDProfileCancelledBy",
                "CancelledReason",
                "TreatmentFunction",
                "SourceOfAdmission",
                "AdministrativeCategory",
                "AdmissionMethod",
                "ReAdmission",
                "DateDischarged",
                "IDProfileResponsibleClinician",
                "IDDischargedBy",
                "IDProfileDischargedBy",
                "TreatmentStatus",
                "MethodOfDischarge",
                "DestinationAfterDischarge",
                "DateDeleted",
                "IDHospitalWaitingList",
                "IDReferralIn",
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

    public CsvCell getDateAdmissionCreated() {
        return super.getCell("DateAdmissionCreated");
    }

    public CsvCell getIdProfileCreatedBy() {
        return super.getCell("IdProfileCreatedBy");
    }

    public CsvCell getIntendedManagement() {
        return super.getCell("IntendedManagement");
    }

    public CsvCell getAdmissionConfirmed() {
        return super.getCell("AdmissionConfirmed");
    }

    public CsvCell getAdmissionLetterPrinted() {
        return super.getCell("AdmissionLetterPrinted");
    }

    public CsvCell getDateAdmitted() {
        return super.getCell("DateAdmitted");
    }

    public CsvCell getIDProfileAdmittedBy() {
        return super.getCell("IDProfileAdmittedBy");
    }

    public CsvCell getAdmissionNotes() {
        return super.getCell("AdmissionNotes");
    }

    public CsvCell getDateCancelled() {
        return super.getCell("DateCancelled");
    }

    public CsvCell getIDProfileCancelledBy() {
        return super.getCell("IDProfileCancelledBy");
    }

    public CsvCell getCancelledReason() {
        return super.getCell("CancelledReason");
    }

    public CsvCell getTreatmentFunction() {
        return super.getCell("TreatmentFunction");
    }

    public CsvCell getSourceOfAdmission() {
        return super.getCell("SourceOfAdmission");
    }

    public CsvCell getAdministrativeCategory() {
        return super.getCell("AdministrativeCategory");
    }

    public CsvCell getAdmissionMethod() {
        return super.getCell("AdmissionMethod");
    }

    public CsvCell getReAdmission() {
        return super.getCell("ReAdmission");
    }

    public CsvCell getDateDischarged() {
        return super.getCell("DateDischarged");
    }

    public CsvCell getIDProfileResponsibleClinician() {
        return super.getCell("IDProfileResponsibleClinician");
    }

    public CsvCell getIDDischargedBy() {
        return super.getCell("IDDischargedBy");
    }

    public CsvCell getIDProfileDischargedBy() {
        return super.getCell("IDProfileDischargedBy");
    }

    public CsvCell getTreatmentStatus() {
        return super.getCell("TreatmentStatus");
    }

    public CsvCell getMethodOfDischarge() {
        return super.getCell("MethodOfDischarge");
    }

    public CsvCell getDestinationAfterDischarge() {
        return super.getCell("DestinationAfterDischarge");
    }

    public CsvCell getDateDeleted() {
        return super.getCell("DateDeleted");
    }

    public CsvCell getIDHospitalWaitingList() {
        return super.getCell("IDHospitalWaitingList");
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


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
