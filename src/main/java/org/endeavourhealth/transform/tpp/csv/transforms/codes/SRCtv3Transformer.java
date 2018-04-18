package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppCtv3LookupDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppCtv3Lookup;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRCtv3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRCtv3Transformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRCtv3Transformer.class);

    private static TppCtv3LookupDalI repository = DalProvider.factoryTppCtv3LookupDal();
    public static final String ROW_ID = "RowId";
    public static final String CTV3_CODE = "ctv3Code";
    public static final String CTV3_TEXT = "ctv3Text";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler) throws Exception {

        AbstractCsvParser parser = parsers.get(SRCtv3.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRCtv3) parser, fhirResourceFiler);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(SRCtv3 parser, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell ctv3Code = parser.getCtv3Code();
        CsvCell ctv3Text = parser.getCtv3Text();

        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();

        auditWrapper.auditValue(rowId.getRowAuditId(), rowId.getColIndex(), ROW_ID);
        auditWrapper.auditValue(ctv3Code.getRowAuditId(), ctv3Code.getColIndex(), CTV3_CODE);
        auditWrapper.auditValue(ctv3Text.getRowAuditId(), ctv3Text.getColIndex(), CTV3_TEXT);

        TppCtv3Lookup lookup = new TppCtv3Lookup(rowId.getLong(),
                ctv3Code.getString(),
                ctv3Text.getString(),
                auditWrapper);

        //save to the DB
        repository.save(lookup);

    }
}
