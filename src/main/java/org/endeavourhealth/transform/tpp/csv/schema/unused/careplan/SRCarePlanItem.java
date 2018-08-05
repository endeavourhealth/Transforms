package org.endeavourhealth.transform.tpp.csv.schema.unused.careplan;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRCarePlanItem extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRCarePlanItem.class);

    public SRCarePlanItem(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "RowIdentifier",
                "IDOrganisationVisibleTo",
                "DateAdded",
                "Instruction",
                "DateRemoved",
                "IDAddedCreatedBy",
                "IDProfileEnteredBy",
                "IDProfileRemovedBy",
                "IDCarePlan",
                "IDReferralIn",
                "IDEvent",
                "IDPatient",
                "IDOrganisation",
                "RemovedData"


        };

    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getDateAdded() {
        return super.getCell("DateAdded");
    }

    public CsvCell getInstruction() {
        return super.getCell("Instruction");
    }

    public CsvCell getDateRemoved() {
        return super.getCell("DateRemoved");
    }

    public CsvCell getIDAddedCreatedBy() {
        return super.getCell("IDAddedCreatedBy");
    }

    public CsvCell getIDProfileEnteredBy() {
        return super.getCell("IDProfileEnteredBy");
    }

    public CsvCell getIDProfileRemovedBy() {
        return super.getCell("IDProfileRemovedBy");
    }

    public CsvCell getIDCarePlan() {
        return super.getCell("IDCarePlan");
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

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    //fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {
        return "TPP SRCarePlanItem Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
