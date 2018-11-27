package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class NOMREF extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(NOMREF.class);

    public NOMREF(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#NOMENCLATURE_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "MNEMONIC_TXT",
                "VALUE_TXT",
                "DISPLAY_TXT",
                "DESCRIPTION_TXT",
                "NOMEN_TYPE_CD",
                "VOCABULARY_CD",
                "CONCEPT_CKI_IDENT",
                "BEG_EFFECTIVE_DT_TM",
                "END_EFFECTIVE_DT_TM"
        };
    }

    public CsvCell getNomenclatureId() {
        return getCell("#NOMENCLATURE_ID");
    }

    public CsvCell getActiveIndicator() {
        return getCell("ACTIVE_IND");
    }

    public CsvCell getMnemonicText() {
        return getCell("MNEMONIC_TXT");
    }

    public CsvCell getValueText() {
        return getCell("VALUE_TXT");
    }

    public CsvCell getDisplayText() {
       return getCell("DISPLAY_TXT");
    }

    public CsvCell getDescriptionText() {
        return getCell("DESCRIPTION_TXT");
    }

    public CsvCell getNomenclatureTypeCode() {
        return getCell("NOMEN_TYPE_CD");
    }

    public CsvCell getVocabularyCode() {
        return getCell("VOCABULARY_CD");
    }

    public CsvCell getConceptIdentifer() {
        return getCell("CONCEPT_CKI_IDENT");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }


}
