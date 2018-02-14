package org.endeavourhealth.transform.emis.csv.transforms.coding;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCsvCodeMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.database.dal.reference.SnomedDalI;
import org.endeavourhealth.core.database.dal.reference.models.SnomedLookup;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCode;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public abstract class ClinicalCodeTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ClinicalCodeTransformer.class);

    private static SnomedDalI repository = DalProvider.factorySnomedDal();

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //because we have to hit a third party web resource, we use a thread pool to support
        //threading these calls to improve performance
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 50000);

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        try {
            AbstractCsvParser parser = parsers.get(ClinicalCode.class);
            while (parser.nextRecord()) {

                try {
                    transform((ClinicalCode)parser, fhirResourceFiler, csvHelper, threadPool, version);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
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
        WebServiceLookup callable = (WebServiceLookup)first.getCallable();
        Throwable exception = first.getException();
        CsvCurrentState parserState = callable.getParserState();
        throw new TransformException(parserState.toString(), exception);
    }

    private static void transform(ClinicalCode parser,
                                  FhirResourceFiler fhirResourceFiler,
                                  EmisCsvHelper csvHelper,
                                  ThreadPool threadPool,
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

        //the parent code ID was added after 5.3
        CsvCell parentCodeId = null;
        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_4)) {
            parentCodeId = parser.getParentCodeId();
        }

        //spin the remainder of our work off to a small thread pool, so we can perform multiple snomed term lookups in parallel
        List<ThreadPoolError> errors = threadPool.submit(new WebServiceLookup(parser.getCurrentState(), codeId,
                                                            emisTerm, emisCode, snomedConceptId, snomedDescriptionId, emisCategory,
                                                            nationalCode, nationalCodeCategory, nationalCodeDescription,
                                                            parentCodeId, csvHelper));
        handleErrors(errors);
    }


    static class WebServiceLookup implements Callable {

        private CsvCurrentState parserState = null;
        private CsvCell codeId = null;
        private CsvCell readTerm = null;
        private CsvCell readCode = null;
        private CsvCell snomedConceptId = null;
        private CsvCell snomedDescriptionId = null;
        private CsvCell emisCategory = null;
        private CsvCell nationalCode = null;
        private CsvCell nationalCodeCategory = null;
        private CsvCell nationalCodeDescription = null;
        private CsvCell parentCodeId = null;
        private EmisCsvHelper csvHelper = null;

        public WebServiceLookup(CsvCurrentState parserState,
                                CsvCell codeId,
                                CsvCell readTerm,
                                CsvCell readCode,
                                CsvCell snomedConceptId,
                                CsvCell snomedDescriptionId,
                                CsvCell emisCategory,
                                CsvCell nationalCode,
                                CsvCell nationalCodeCategory,
                                CsvCell nationalCodeDescription,
                                CsvCell parentCodeId,
                                EmisCsvHelper csvHelper) {

            this.parserState = parserState;
            this.codeId = codeId;
            this.readTerm = readTerm;
            this.readCode = readCode;
            this.snomedConceptId = snomedConceptId;
            this.snomedDescriptionId = snomedDescriptionId;
            this.emisCategory = emisCategory;
            this.nationalCode = nationalCode;
            this.nationalCodeCategory = nationalCodeCategory;
            this.nationalCodeDescription = nationalCodeDescription;
            this.parentCodeId = parentCodeId;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {
                ClinicalCodeType codeType = ClinicalCodeType.fromValue(emisCategory.getString());

                ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();

                auditWrapper.auditValue(readCode.getRowAuditId(), readCode.getColIndex(), EmisCodeHelper.AUDIT_CLINICAL_CODE_READ_CODE);
                auditWrapper.auditValue(readTerm.getRowAuditId(), readTerm.getColIndex(), EmisCodeHelper.AUDIT_CLINICAL_CODE_READ_TERM);
                auditWrapper.auditValue(snomedConceptId.getRowAuditId(), snomedConceptId.getColIndex(), EmisCodeHelper.AUDIT_CLINICAL_CODE_SNOMED_CONCEPT_ID);
                auditWrapper.auditValue(snomedDescriptionId.getRowAuditId(), snomedDescriptionId.getColIndex(), EmisCodeHelper.AUDIT_CLINICAL_CODE_SNOMED_DESCRIPTION_ID);

                //see if we can find an official snomed term for the concept ID
                String snomedTerm = null;
                SnomedLookup snomedLookup = repository.getSnomedLookup(snomedConceptId.getString());
                if (snomedLookup != null) {
                    snomedTerm = snomedLookup.getTerm();
                }

                Long parentCodeIdValue = null;
                if (parentCodeId != null) {
                    parentCodeIdValue = parentCodeId.getLong();
                }

                EmisCsvCodeMap mapping = new EmisCsvCodeMap();
                mapping.setMedication(false);
                mapping.setCodeId(codeId.getLong());
                mapping.setCodeType(codeType.getValue());
                mapping.setReadTerm(readTerm.getString());
                mapping.setReadCode(readCode.getString());
                mapping.setSnomedConceptId(snomedConceptId.getLong());
                mapping.setSnomedDescriptionId(snomedDescriptionId.getLong());
                mapping.setSnomedTerm(snomedTerm);
                mapping.setNationalCode(nationalCode.getString());
                mapping.setNationalCodeCategory(nationalCodeCategory.getString());
                mapping.setNationalCodeDescription(nationalCodeDescription.getString());
                mapping.setParentCodeId(parentCodeIdValue);
                mapping.setAudit(auditWrapper);

                //save to the DB
                csvHelper.saveClinicalOrDrugCode(mapping);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }

        public CsvCurrentState getParserState() {
            return parserState;
        }
    }
}
