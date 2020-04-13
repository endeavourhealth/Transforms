package org.endeavourhealth.transform.emis.csv.schema.coding;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class ClinicalCode extends AbstractCsvParser {

    public ClinicalCode(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_4)) {
            return new String[]{
                    "CodeId",
                    "Term",
                    "ReadTermId",
                    "SnomedCTConceptId",
                    "SnomedCTDescriptionId",
                    "NationalCode",
                    "NationalCodeCategory",
                    "NationalDescription",
                    "EmisCodeCategoryDescription",
                    "ProcessingId",
                    "ParentCodeId"
            };

        } else {

            return new String[]{
                    "CodeId",
                    "Term",
                    "ReadTermId",
                    "SnomedCTConceptId",
                    "SnomedCTDescriptionId",
                    "NationalCode",
                    "NationalCodeCategory",
                    "NationalDescription",
                    "EmisCodeCategoryDescription",
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
    public CsvCell getReadTermId() {
        return super.getCell("ReadTermId");
    }
    public CsvCell getSnomedCTConceptId() {
        return super.getCell("SnomedCTConceptId");
    }
    public CsvCell getSnomedCTDescriptionId() {
        return super.getCell("SnomedCTDescriptionId");
    }
    public CsvCell getNationalCode() {
        return super.getCell("NationalCode");
    }
    public CsvCell getNationalCodeCategory() {
        return super.getCell("NationalCodeCategory");
    }
    public CsvCell getNationalDescription() {
        return super.getCell("NationalDescription");
    }
    public CsvCell getEmisCodeCategoryDescription() {
        return super.getCell("EmisCodeCategoryDescription");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }
    public CsvCell getParentCodeId() {
        return super.getCell("ParentCodeId");
    }

    /*public Long getCodeId() {
        return super.getLong("CodeId");
    }
    public String getTerm() {
        return super.getString("Term");
    }
    public String getReadTermId() {
        return super.getString("ReadTermId");
    }
    public Long getSnomedCTConceptId() {
        return super.getLong("SnomedCTConceptId");
    }
    public Long getSnomedCTDescriptionId() {
        return super.getLong("SnomedCTDescriptionId");
    }
    public String getNationalCode() {
        return super.getString("NationalCode");
    }
    public String getNationalCodeCategory() {
        return super.getString("NationalCodeCategory");
    }
    public String getNationalDescription() {
        return super.getString("NationalDescription");
    }
    public String getEmisCodeCategoryDescription() {
        return super.getString("EmisCodeCategoryDescription");
    }
    public Integer getProcessingId() {
        return super.getInt("ProcessingId");
    }
    public Long getParentCodeId() {
        return super.getLong("ParentCodeId");
    }*/
}
