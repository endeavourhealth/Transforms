package org.endeavourhealth.transform.homerton.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.endeavourhealth.transform.homerton.schema.CodeNHSAliasTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CodeNHSAliasTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(CodeNHSAliasTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper) throws Exception {


        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file is a critical error. A bad entry here could have multiple serious effects

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    transform((CodeNHSAliasTable) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void transform(CodeNHSAliasTable parser, FhirResourceFiler fhirResourceFiler, HomertonCsvHelper csvHelper) throws Exception {

        //For CodeTable the first column should always resolve as a numeric code. We've seen some bad data appended to CodeTable files
        if (!StringUtils.isNumeric(parser.getCodeValueCode().getString())) {
                    return;
        }

        // cache the NHS Alias here to use in the Codes Tranformer
        CsvCell codeValueCode = parser.getCodeValueCode();
        CsvCell codeValueNHSAliasCell = parser.getCodeValueNHSAlias();
        csvHelper.cacheCodeNHSAlias (codeValueCode.getString(), codeValueNHSAliasCell);
    }
 }
