package org.endeavourhealth.transform.tpp.csv.schema.codes;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRCtv3ToSnomed extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRCtv3ToSnomed.class);

    public SRCtv3ToSnomed(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_92)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "Ctv3Code",
                    "SnomedCode"
            };

        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "Ctv3Code",
                    "SnomedCode",
                    "RemovedData"
            };
        }
    }

    public CsvCell getCtv3Code() {
        return super.getCell("Ctv3Code");
    }

    public CsvCell getSnomedCode() {
        return super.getCell("SnomedCode");
    }


    @Override
    protected boolean isFileAudited() {
        //this file is just used to populate a lookup table, so don't audit
        return false;
    }
}
