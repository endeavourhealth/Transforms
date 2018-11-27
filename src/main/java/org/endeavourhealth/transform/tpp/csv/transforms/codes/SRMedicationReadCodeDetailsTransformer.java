package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppMultiLexToCtv3MapDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMultiLexToCtv3Map;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRMedicationReadCodeDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRMedicationReadCodeDetailsTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRMedicationReadCodeDetailsTransformer.class);

    private static TppMultiLexToCtv3MapDalI repository = DalProvider.factoryTppMultiLexToCtv3MapDal();
    public static final String ROW_ID = "RowId";
    public static final String MULTILEX_PRODUCT_ID = "multiLexProductId";
    public static final String CTV3_READ_CODE = "ctv3ReadCode";
    public static final String CTV3_READ_TERM = "ctv3ReadTerm";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler) throws Exception {

        AbstractCsvParser parser = parsers.get(SRMedicationReadCodeDetails.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRMedicationReadCodeDetails) parser, fhirResourceFiler);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(SRMedicationReadCodeDetails parser, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell multiLexProductId = parser.getIDMultiLexProduct();
        CsvCell ctv3ReadCode = parser.getDrugReadCode();
        CsvCell ctv3ReadTerm = parser.getDrugReadCodeDesc();


        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();

        auditWrapper.auditValue(rowId.getPublishedFileId(), rowId.getRecordNumber(), rowId.getColIndex(), ROW_ID);
        auditWrapper.auditValue(multiLexProductId.getPublishedFileId(), multiLexProductId.getRecordNumber(), multiLexProductId.getColIndex(), MULTILEX_PRODUCT_ID);
        auditWrapper.auditValue(ctv3ReadCode.getPublishedFileId(), ctv3ReadCode.getRecordNumber(), ctv3ReadCode.getColIndex(), CTV3_READ_CODE);
        auditWrapper.auditValue(ctv3ReadTerm.getPublishedFileId(), ctv3ReadTerm.getRecordNumber(), ctv3ReadTerm.getColIndex(), CTV3_READ_TERM);

        TppMultiLexToCtv3Map mapping = new TppMultiLexToCtv3Map(rowId.getLong(),
                multiLexProductId.getLong(),
                ctv3ReadCode.getString(),
                ctv3ReadTerm.getString(),
                auditWrapper);

        //save to the DB
        repository.save(mapping);
    }
}
