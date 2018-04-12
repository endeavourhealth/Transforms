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
        return new String[]{
                "RowIdentifier",
                "IDMultiLexProduct",
                "DrugReadCode",
                "DrugReadCodeDesc",
                "RemovedData"
        };
    }

    public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
    public CsvCell getIDMultiLexProduct() { return super.getCell("IDMultiLexProduct");};
    public CsvCell getDrugReadCode() { return super.getCell("DrugReadCode");};
    public CsvCell getDrugReadCodeDesc() { return super.getCell("DrugReadCodeDesc");};
    public CsvCell getRemovedData() { return super.getCell("RemovedData");};


    //TODO fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {return "TPP Medication Read Code Details Entry file ";}

    @Override
    protected boolean isFileAudited() {return true;}
}
