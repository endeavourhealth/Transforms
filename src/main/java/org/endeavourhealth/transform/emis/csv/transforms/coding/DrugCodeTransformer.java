package org.endeavourhealth.transform.emis.csv.transforms.coding;

import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisTransformDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.coding.DrugCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class DrugCodeTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DrugCodeTransformer.class);

    private static EmisTransformDalI mappingDal = DalProvider.factoryEmisTransformDal();

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        try {
            List<EmisCsvCodeMap> mappingsToSave = new ArrayList<>();

            AbstractCsvParser parser = parsers.get(DrugCode.class);
            while (parser != null && parser.nextRecord()) {

                try {
                    transform((DrugCode)parser, fhirResourceFiler, csvHelper, mappingsToSave);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }

            //and save any still pending
            if (!mappingsToSave.isEmpty()) {
                csvHelper.submitToThreadPool(new Task(mappingsToSave));
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    private static void transform(DrugCode parser,
                                  FhirResourceFiler fhirResourceFiler,
                                  EmisCsvHelper csvHelper,
                                  List<EmisCsvCodeMap> mappingsToSave) throws Exception {

        CsvCell codeId = parser.getCodeId();
        CsvCell term = parser.getTerm();
        CsvCell dmdId = parser.getDmdProductCodeId();

        EmisCsvCodeMap mapping = new EmisCsvCodeMap();
        mapping.setMedication(true);
        mapping.setCodeId(codeId.getLong());
        mapping.setSnomedConceptId(dmdId.getLong());
        mapping.setSnomedTerm(term.getString());

        //we need to generate the audit of the source cells to FHIR so we can apply it when we create resources
        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();
        if (!dmdId.isEmpty()) {
            auditWrapper.auditValue(dmdId.getPublishedFileId(), dmdId.getRecordNumber(), dmdId.getColIndex(), EmisCodeHelper.AUDIT_DRUG_CODE);
        }
        auditWrapper.auditValue(term.getPublishedFileId(), term.getRecordNumber(), term.getColIndex(), EmisCodeHelper.AUDIT_DRUG_TERM);
        mapping.setAudit(auditWrapper);

        mappingsToSave.add(mapping);
        if (mappingsToSave.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
            List<EmisCsvCodeMap> copy = new ArrayList<>(mappingsToSave);
            mappingsToSave.clear();
            csvHelper.submitToThreadPool(new Task(copy));
        }
    }

    private static void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since they'll most-likely be the same
        ThreadPoolError first = errors.get(0);
        Throwable exception = first.getException();
        throw new TransformException("", exception);
    }


    static class Task implements Callable {

        private List<EmisCsvCodeMap> mappings = null;

        public Task(List<EmisCsvCodeMap> mappings) {
            this.mappings = mappings;
        }

        @Override
        public Object call() throws Exception {

            try {
                //save the mapping batch
                mappingDal.saveCodeMappings(mappings);

            } catch (Throwable t) {
                String msg = "Error saving drug code records for code IDs ";
                for (EmisCsvCodeMap mapping: mappings) {
                    msg += mapping.getCodeId();
                    msg += ", ";
                }

                LOG.error(msg, t);
                throw new TransformException(msg, t);
            }

            return null;
        }
    }
}
