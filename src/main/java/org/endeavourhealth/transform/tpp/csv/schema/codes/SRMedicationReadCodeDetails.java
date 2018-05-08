package org.endeavourhealth.transform.tpp.csv.schema.codes;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRMedicationReadCodeDetails extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRMedicationReadCodeDetails.class);

    public SRMedicationReadCodeDetails(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        //TODO - update transform to check for null cells when using fields not in the older version
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_87)
                || version.equals(TppCsvToFhirTransformer.VERSION_88)) {
            return new String[]{
                    "RowIdentifier",
                    "IDMultiLexProduct",
                    "DrugReadCode",
                    "DrugReadCodeDesc"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_89)) {
            return new String[]{
                    "RowIdentifier",
                    "IDMultiLexProduct",
                    "DrugReadCode",
                    "DrugReadCodeDesc",
                    "RemovedData"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDMultiLexProduct",
                    "DrugReadCode",
                    "DrugReadCodeDesc"
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDMultiLexProduct() {
        return super.getCell("IDMultiLexProduct");
    }

    public CsvCell getDrugReadCode() {
        return super.getCell("DrugReadCode");
    }

    public CsvCell getDrugReadCodeDesc() {
        return super.getCell("DrugReadCodeDesc");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    @Override
    protected String getFileTypeDescription() {
        return "TPP Medication Read Code Details Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
