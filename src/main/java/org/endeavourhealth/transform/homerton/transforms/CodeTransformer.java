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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class CodeTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(CodeTransformer.class);

    private static CernerCodeValueRefDalI repository = DalProvider.factoryCernerCodeValueRefDal();
    public static final String CODE_VALUE = "Codeval";
    public static final String CODE_SET_NBR = "CodeSetNr";
    public static final String DISP_TXT = "DispTxt";
    public static final String DESC_TXT = "DescTxt";
    public static final String MEANING_TXT = "MeanTxt";
    private final static String DECIMAL_POINT = ".";

    //private static SimpleDateFormat formatDaily = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    //private static SimpleDateFormat formatBulk = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");

    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper,
                                 String primaryOrgOdsCode) throws Exception {


        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file is a critical error. A bad entry here could have multiple serious effects

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    transform((CodeTable) parser, fhirResourceFiler);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }


    public static void transform(CodeTable parser, FhirResourceFiler fhirResourceFiler) throws Exception {
        //For CodeTable the first column should always resolve as a numeric code. We've seen some bad data appended to CodeTable files
        if (!StringUtils.isNumeric(parser.getCodeValueCode().getString())) {
                    return;
        }
        CsvCell codeValueCode = parser.getCodeValueCode();
        CsvCell date = parser.getDate();
        CsvCell activeInd = parser.getActiveInd();
        //CsvCell codeDescTxt = parser.getCodeDescTxt();
        CsvCell codeDispTxt = parser.getCodeDispTxt();
        CsvCell codeMeaningTxt = parser.getCodeMeaningTxt();
        CsvCell codeSetNbr = parser.getCodeSetNbr();
        //CsvCell codeSetDescTxt = parser.getCodeSetDescTxt();
        //CsvCell aliasNhsCdAlias = parser.getAliasNhsCdAlias();

        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();
        auditWrapper.auditValue(codeValueCode.getRowAuditId(), codeValueCode.getColIndex(), CODE_VALUE);
        auditWrapper.auditValue(codeSetNbr.getRowAuditId(), codeSetNbr.getColIndex(), CODE_SET_NBR);
        auditWrapper.auditValue(codeDispTxt.getRowAuditId(), codeDispTxt.getColIndex(), DISP_TXT);
        //auditWrapper.auditValue(codeDescTxt.getRowAuditId(), codeDescTxt.getColIndex(), DESC_TXT);
        auditWrapper.auditValue(codeMeaningTxt.getRowAuditId(), codeMeaningTxt.getColIndex(), MEANING_TXT);

        byte active = (byte)activeInd.getInt().intValue();
        CernerCodeValueRef mapping = new CernerCodeValueRef(codeValueCode.getString(),
                                    date.getDate(),
                                    active,
                                    "",
                                    codeDispTxt.getString(),
                                    codeMeaningTxt.getString(),
                                    codeSetNbr.getLong(),
                                    "",
                                    "",
                                    fhirResourceFiler.getServiceId().toString(),
                                    auditWrapper);


                //save to the DB
            repository.save(mapping, fhirResourceFiler.getServiceId());


    }
 }
