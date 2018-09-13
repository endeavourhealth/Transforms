package org.endeavourhealth.transform.tpp.csv.schema.unused.careplan;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRCarePlanSkillset extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRCarePlanSkillset.class);

    public SRCarePlanSkillset(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "IDCarePlan",
                "SkillSetRequired",
                "DateAdded",
                "IDProfileAddedBy",
                "DateRemoved",
                "IDProfileRemovedBy",
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

    public CsvCell getIDCarePlan() {
        return super.getCell("IDCarePlan");
    }

    public CsvCell getSkillSetRequired() {
        return super.getCell("SkillSetRequired");
    }

    public CsvCell getDateAdded() {
        return super.getCell("DateAdded");
    }

    public CsvCell getIDProfileAddedBy() {
        return super.getCell("IDProfileAddedBy");
    }

    public CsvCell getDateRemoved() {
        return super.getCell("DateRemoved");
    }

    public CsvCell getIDProfileRemovedBy() {
        return super.getCell("IDProfileRemovedBy");
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


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
