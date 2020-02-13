package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppMultiLexToCtv3MapDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMultiLexToCtv3Map;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.database.dal.reference.models.SnomedLookup;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRMedicationReadCodeDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class SRMedicationReadCodeDetailsTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRMedicationReadCodeDetailsTransformer.class);

    private static TppMultiLexToCtv3MapDalI repository = DalProvider.factoryTppMultiLexToCtv3MapDal();
    public static final String ROW_ID = "RowId";
    public static final String MULTILEX_PRODUCT_ID = "multiLexProductId";
    public static final String CTV3_READ_CODE = "ctv3ReadCode";
    public static final String CTV3_READ_TERM = "ctv3ReadTerm";

    public static void transform(Map<Class, AbstractCsvParser> parsers, FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper) throws Exception {

        try {
            List<TppMultiLexToCtv3Map> mappingsToSave = new ArrayList<>();


            AbstractCsvParser parser = parsers.get(SRMedicationReadCodeDetails.class);
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        createResource((SRMedicationReadCodeDetails) parser, fhirResourceFiler, csvHelper, mappingsToSave);

                    } catch (Exception ex) {
                        throw new TransformException(parser.getCurrentState().toString(), ex);
                    }
                }
            }

            //and save any still pending
            if (!mappingsToSave.isEmpty()) {
                csvHelper.submitToThreadPool(new Task(mappingsToSave));
            }

            //call this to abort if we had any errors, during the above processing
            fhirResourceFiler.failIfAnyErrors();

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }

    }

    public static void createResource(SRMedicationReadCodeDetails parser, FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper, List<TppMultiLexToCtv3Map> mappingsToSave) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell multiLexProductId = parser.getIDMultiLexProduct();
        CsvCell ctv3ReadCode = parser.getDrugReadCode();
        CsvCell ctv3ReadTerm = parser.getDrugReadCodeDesc();

        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();

        auditWrapper.auditValue(rowId.getPublishedFileId(), rowId.getRecordNumber(), rowId.getColIndex(), ROW_ID);
        auditWrapper.auditValue(multiLexProductId.getPublishedFileId(), multiLexProductId.getRecordNumber(), multiLexProductId.getColIndex(), MULTILEX_PRODUCT_ID);
        auditWrapper.auditValue(ctv3ReadCode.getPublishedFileId(), ctv3ReadCode.getRecordNumber(), ctv3ReadCode.getColIndex(), CTV3_READ_CODE);
        auditWrapper.auditValue(ctv3ReadTerm.getPublishedFileId(), ctv3ReadTerm.getRecordNumber(), ctv3ReadTerm.getColIndex(), CTV3_READ_TERM);

        TppMultiLexToCtv3Map mapping = new TppMultiLexToCtv3Map(rowId.getLong().longValue(),
                multiLexProductId.getLong().longValue(),
                ctv3ReadCode.getString(),
                ctv3ReadTerm.getString(),
                auditWrapper);
        mappingsToSave.add(mapping);

        if (mappingsToSave.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
            List<TppMultiLexToCtv3Map> copy = new ArrayList<>(mappingsToSave);
            mappingsToSave.clear();
            csvHelper.submitToThreadPool(new Task(copy));
        }
    }

    static class Task implements Callable {

        private List<TppMultiLexToCtv3Map> mappings = null;

        public Task(List<TppMultiLexToCtv3Map> mappings) {
            this.mappings = mappings;
        }

        @Override
        public Object call() throws Exception {

            try {
                //save to the DB
                repository.save(mappings);

            } catch (Throwable t) {
                String msg = "Error saving TPPMultilexToCTV3 records for row IDs ";
                for (TppMultiLexToCtv3Map mapping: mappings) {
                    msg += mapping.getRowId();
                    msg += ", ";
                }
                LOG.error(msg, t);
                throw new TransformException(msg, t);
            }

            return null;
        }
    }
}
