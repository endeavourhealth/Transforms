package org.endeavourhealth.transform.homerton.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.endeavourhealth.transform.homerton.schema.CodeTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CodeTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(CodeTransformer.class);

    private static CernerCodeValueRefDalI repository = DalProvider.factoryCernerCodeValueRefDal();
    public static final String CODE_VALUE = "Codeval";
    public static final String CODE_SET_NBR = "CodeSetNr";
    public static final String DISP_TXT = "DispTxt";
    public static final String DESC_TXT = "DescTxt";
    public static final String MEANING_TXT = "MeanTxt";
    public static final String CODE_NHS_ALIAS = "NHSAliasCode";

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper) throws Exception {


        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file is a critical error. A bad entry here could have multiple serious effects

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    transform((CodeTable) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void transform(CodeTable parser, FhirResourceFiler fhirResourceFiler, HomertonCsvHelper csvHelper) throws Exception {
        //For CodeTable the first column should always resolve as a numeric code. We've seen some bad data appended to CodeTable files
        if (!StringUtils.isNumeric(parser.getCodeValueCode().getString())) {
                    return;
        }
        CsvCell codeValueCode = parser.getCodeValueCode();
        CsvCell date = parser.getDate();
        CsvCell activeInd = parser.getActiveInd();
        CsvCell codeDescTxt = parser.getCodeDescTxt();
        CsvCell codeDispTxt = parser.getCodeDispTxt();
        CsvCell codeMeaningTxt = parser.getCodeMeaningTxt();
        CsvCell codeSetNbr = parser.getCodeSetNbr();

        String codeNHSAlias = "";
        CsvCell aliasNhsCdAliasCell = csvHelper.findCodeNHSAlias(codeValueCode.getString());
        if (aliasNhsCdAliasCell != null) {
            codeNHSAlias = aliasNhsCdAliasCell.getString();
        }

        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();
        auditWrapper.auditValue(codeValueCode.getPublishedFileId(), codeValueCode.getRecordNumber(), codeValueCode.getColIndex(), CODE_VALUE);
        auditWrapper.auditValue(codeSetNbr.getPublishedFileId(), codeSetNbr.getRecordNumber(), codeSetNbr.getColIndex(), CODE_SET_NBR);
        auditWrapper.auditValue(codeDispTxt.getPublishedFileId(), codeDispTxt.getRecordNumber(), codeDispTxt.getColIndex(), DISP_TXT);
        auditWrapper.auditValue(codeDescTxt.getPublishedFileId(), codeDescTxt.getRecordNumber(), codeDescTxt.getColIndex(), DESC_TXT);
        auditWrapper.auditValue(codeMeaningTxt.getPublishedFileId(), codeMeaningTxt.getRecordNumber(), codeMeaningTxt.getColIndex(), MEANING_TXT);
        if (aliasNhsCdAliasCell != null) {
            auditWrapper.auditValue(aliasNhsCdAliasCell.getPublishedFileId(), aliasNhsCdAliasCell.getRecordNumber(), aliasNhsCdAliasCell.getColIndex(), CODE_NHS_ALIAS);
        }

        byte active = (byte)activeInd.getInt().intValue();
        CernerCodeValueRef mapping = new CernerCodeValueRef(codeValueCode.getString(),
                                    date.getDate(),
                                    active,
                                    codeDescTxt.getString(),
                                    codeDispTxt.getString(),
                                    codeMeaningTxt.getString(),
                                    codeSetNbr.getLong(),
                                    "",
                                    codeNHSAlias,
                                    fhirResourceFiler.getServiceId(),
                                    auditWrapper);

                //save to the DB
            repository.saveCVREF(mapping);
    }
 }
