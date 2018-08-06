package org.endeavourhealth.transform.tpp.csv.schema.unused.hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRHospitalAlertIndicator extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRHospitalAlertIndicator.class);

    public SRHospitalAlertIndicator(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "AlertType",
                "AlertCode",
                "AlertTextual",
                "HiddenWardView",
                "LatexAllergy",
                "DNAR",
                "RecurringAlert",
                "DateCreated",
                "DateDeleted",
                "IDOrganisation"


        };

    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getAlertType() {
        return super.getCell("AlertType");
    }

    public CsvCell getAlertCode() {
        return super.getCell("AlertCode");
    }

    public CsvCell getAlertTextual() {
        return super.getCell("AlertTextual");
    }

    public CsvCell getHiddenWardView() {
        return super.getCell("HiddenWardView");
    }

    public CsvCell getLatexAllergy() {
        return super.getCell("LatexAllergy");
    }

    public CsvCell getDNAR() {
        return super.getCell("DNAR");
    }

    public CsvCell getRecurringAlert() {
        return super.getCell("RecurringAlert");
    }

    public CsvCell getDateCreated() {
        return super.getCell("DateCreated");
    }

    public CsvCell getDateDeleted() {
        return super.getCell("DateDeleted");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }


    //fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {
        return "TPP SRHospitalAlertIndicator Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
