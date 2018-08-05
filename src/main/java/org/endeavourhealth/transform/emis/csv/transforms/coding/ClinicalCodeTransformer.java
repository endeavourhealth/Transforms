package org.endeavourhealth.transform.emis.csv.transforms.coding;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisTransformDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.database.dal.reference.SnomedDalI;
import org.endeavourhealth.core.database.dal.reference.models.SnomedLookup;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCode;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public abstract class ClinicalCodeTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ClinicalCodeTransformer.class);

    private static SnomedDalI snomedDal = DalProvider.factorySnomedDal();
    private static EmisTransformDalI mappingDal = DalProvider.factoryEmisTransformDal();

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //we're just streaming content, row by row, into the DB, so use a threadpool to parallelise it
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 50000);

        try {
            List<EmisCsvCodeMap> mappingsToSave = new ArrayList<>();

            AbstractCsvParser parser = parsers.get(ClinicalCode.class);
            while (parser.nextRecord()) {

                //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
                //to parse any record in this file it a critical error
                try {
                    transform((ClinicalCode)parser, fhirResourceFiler, csvHelper, threadPool, mappingsToSave, version);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }

            //and save any still pending
            if (!mappingsToSave.isEmpty()) {
                List<ThreadPoolError> errors = threadPool.submit(new Task(mappingsToSave));
                handleErrors(errors);
            }

        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            handleErrors(errors);
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

    private static void transform(ClinicalCode parser,
                                  FhirResourceFiler fhirResourceFiler,
                                  EmisCsvHelper csvHelper,
                                  ThreadPool threadPool,
                                  List<EmisCsvCodeMap> mappingsToSave,
                                  String version) throws Exception {

        CsvCell codeId = parser.getCodeId();
        CsvCell emisTerm = parser.getTerm();
        CsvCell emisCode = parser.getReadTermId();
        CsvCell snomedConceptId = parser.getSnomedCTConceptId();
        CsvCell snomedDescriptionId = parser.getSnomedCTDescriptionId();
        CsvCell emisCategory = parser.getEmisCodeCategoryDescription();
        CsvCell nationalCode = parser.getNationalCode();
        CsvCell nationalCodeCategory = parser.getNationalCodeCategory();
        CsvCell nationalCodeDescription = parser.getNationalDescription();

        ClinicalCodeType codeType = ClinicalCodeType.fromValue(emisCategory.getString());

        EmisCsvCodeMap mapping = new EmisCsvCodeMap();
        mapping.setMedication(false);
        mapping.setCodeId(codeId.getLong());
        mapping.setCodeType(codeType.getValue());
        mapping.setReadTerm(emisTerm.getString());
        mapping.setReadCode(emisCode.getString());
        mapping.setSnomedConceptId(snomedConceptId.getLong());
        mapping.setSnomedDescriptionId(snomedDescriptionId.getLong());
        mapping.setNationalCode(nationalCode.getString());
        mapping.setNationalCodeCategory(nationalCodeCategory.getString());
        mapping.setNationalCodeDescription(nationalCodeDescription.getString());

        //the parent code ID was added after 5.3
        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_4)) {
            CsvCell parentCodeId = parser.getParentCodeId();
            mapping.setParentCodeId(parentCodeId.getLong());
        }

        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();
        auditWrapper.auditValue(emisCode.getRowAuditId(), emisCode.getColIndex(), EmisCodeHelper.AUDIT_CLINICAL_CODE_READ_CODE);
        auditWrapper.auditValue(emisTerm.getRowAuditId(), emisTerm.getColIndex(), EmisCodeHelper.AUDIT_CLINICAL_CODE_READ_TERM);
        auditWrapper.auditValue(snomedConceptId.getRowAuditId(), snomedConceptId.getColIndex(), EmisCodeHelper.AUDIT_CLINICAL_CODE_SNOMED_CONCEPT_ID);
        auditWrapper.auditValue(snomedDescriptionId.getRowAuditId(), snomedDescriptionId.getColIndex(), EmisCodeHelper.AUDIT_CLINICAL_CODE_SNOMED_DESCRIPTION_ID);
        mapping.setAudit(auditWrapper);

        mappingsToSave.add(mapping);
        if (mappingsToSave.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
            List<EmisCsvCodeMap> copy = new ArrayList<>(mappingsToSave);
            mappingsToSave.clear();
            List<ThreadPoolError> errors = threadPool.submit(new Task(copy));
            handleErrors(errors);
        }
    }


    static class Task implements Callable {

        private List<EmisCsvCodeMap> mappings = null;

        public Task(List<EmisCsvCodeMap> mappings) {
            this.mappings = mappings;
        }

        @Override
        public Object call() throws Exception {

            try {

                //see if we can find an official snomed term for each concept ID
                for (EmisCsvCodeMap mapping: mappings) {

                    SnomedLookup snomedLookup = snomedDal.getSnomedLookup("" + mapping.getSnomedConceptId());
                    if (snomedLookup != null) {
                        String snomedTerm = snomedLookup.getTerm();
                        mapping.setSnomedTerm(snomedTerm);
                    }
                }

                //and save the mapping batch
                mappingDal.save(mappings);

            } catch (Throwable t) {
                String msg = "Error saving clinical code records for code IDs ";
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
