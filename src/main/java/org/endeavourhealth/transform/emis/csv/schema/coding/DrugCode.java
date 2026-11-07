package org.endeavourhealth.transform.emis.csv.schema.coding;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class DrugCode extends AbstractCsvParser {

    public DrugCode(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if(version.equals(EmisCsvToFhirTransformer.VERSION_8_0)
                || version.equals(EmisCsvToFhirTransformer.VERSION_8_0_1)
                || version.equals(EmisCsvToFhirTransformer.VERSION_8_1)
                || version.equals(EmisCsvToFhirTransformer.VERSION_8_2)
                || version.equals(EmisCsvToFhirTransformer.VERSION_8_3)){//SD:1090
            return new String[]{
                    "CodeId",
                    "Term",
                    "DmdProductCodeId",
                    "BNFChapterRef",
                    "ProcessingId"
            };
        } else {
            return new String[]{
                    "CodeId",
                    "Term",
                    "DmdProductCodeId",
                    "ProcessingId"
            };
        }

    }

    @Override
    protected boolean isFileAudited() {
        //just used to load a lookup table, so don't audit
        return false;
    }

    public CsvCell getCodeId() {
        return super.getCell("CodeId");
    }
    public CsvCell getTerm() {
        return super.getCell("Term");
    }
    public CsvCell getDmdProductCodeId() {
        return super.getCell("DmdProductCodeId");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }

}
