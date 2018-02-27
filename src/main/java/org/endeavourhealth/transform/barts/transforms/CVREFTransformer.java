package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.CVREF;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.Map;

public class CVREFTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(CVREFTransformer.class);

    private static CernerCodeValueRefDalI repository = DalProvider.factoryCernerCodeValueRefDal();
    private static final String CODE_VALUE = "Codeval";
    private static final String CODE_SET_NBR = "CodeSetNr";
    private static final String DISP_TXT = "DispTxt";
    private static final String DESC_TXT = "DescTxt";
    private static final String MEANING_TXT = "MeanTxt";

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {


        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file is a critical error. A bad entry here could have multiple serious effects

        AbstractCsvParser parser = parsers.get(CVREF.class);
        while (parser.nextRecord()) {

            try {
                transform((CVREF) parser, fhirResourceFiler);
            } catch (Exception ex) {
                throw new TransformException(parser.getCurrentState().toString(), ex);
            }
        }
    }



    private static void transform(CVREF parser, FhirResourceFiler fhirResourceFiler ) throws Exception {

        CsvCell codeValueCode = parser.getCodeValueCode();
        CsvCell date = parser.getDate();
        CsvCell activeInd = parser.getActiveInd();
        CsvCell codeDescTxt = parser.getCodeDescTxt();
        CsvCell codeDispTxt = parser.getCodeDispTxt();
        CsvCell codeMeaningTxt = parser.getCodeMeaningTxt();
        CsvCell codeSetNbr = parser.getCodeSetNbr();
        CsvCell codeSetDescTxt = parser.getCodeSetDescTxt();
        CsvCell aliasNhsCdAlias = parser.getAliasNhsCdAlias();

        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();

        auditWrapper.auditValue(codeValueCode.getRowAuditId(), codeValueCode.getColIndex(), CODE_VALUE);
        auditWrapper.auditValue(codeSetNbr.getRowAuditId(), codeSetNbr.getColIndex(), CODE_SET_NBR);
        auditWrapper.auditValue(codeDispTxt.getRowAuditId(), codeDispTxt.getColIndex(), DISP_TXT);
        auditWrapper.auditValue(codeDescTxt.getRowAuditId(), codeDescTxt.getColIndex(), DESC_TXT);
        auditWrapper.auditValue(codeMeaningTxt.getRowAuditId(), codeMeaningTxt.getColIndex(), MEANING_TXT);
        //TODO FYI. CernerCodeValueRef uses sql date. CsvCell uses util date. Deliberate?
        Date mapDate = new java.sql.Date(date.getDate().getTime());
        byte active = (byte)activeInd.getInt().intValue();
        CernerCodeValueRef mapping = new CernerCodeValueRef(codeValueCode.getLong(),
                                    mapDate,
                                    active,
                                    codeDescTxt.getString(),
                                    codeDispTxt.getString(),
                                    codeMeaningTxt.getString(),
                                    codeSetNbr.getLong(),
                                    codeSetDescTxt.getString(),
                                    aliasNhsCdAlias.getString(),
                                    fhirResourceFiler.getServiceId().toString(),
                                    auditWrapper.writeToJson());


                //save to the DB
            repository.save(mapping);


    }
}
