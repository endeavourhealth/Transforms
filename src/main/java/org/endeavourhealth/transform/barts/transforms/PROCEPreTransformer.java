package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingPROCEDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingPROCE;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.PROCE;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class PROCEPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PROCEPreTransformer.class);

    private static StagingPROCEDalI repository = DalProvider.factoryBartsStagingPROCEDalI();
    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            for (ParserI parser : parsers) {
                while (parser.nextRecord()) {
//                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
//                        continue;
//                    }
                    //no try/catch here, since any failure here means we don't want to continue
                    processRecord((PROCE) parser, fhirResourceFiler, csvHelper);
                }
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }

    }

    public static void processRecord(PROCE parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {


        StagingPROCE stagingPROCE = new StagingPROCE();

        stagingPROCE.setActiveInd(parser.getActiveIndicator().getIntAsBoolean());
        stagingPROCE.setExchangeId(parser.getExchangeId().toString());
        stagingPROCE.setDateReceived(new Date());
        int procId = parser.getProcedureID().getInt();
        stagingPROCE.setProcedureId(procId);

        boolean activeInd = parser.getActiveIndicator().getIntAsBoolean();
        stagingPROCE.setActiveInd(activeInd);
        UUID serviceId = csvHelper.getServiceId();
        //only set additional values if active
        if (activeInd) {
            stagingPROCE.setEncounterId(parser.getEncounterId().getInt());
            stagingPROCE.setProcedureDtTm(parser.getProcedureDateTime().getDate());
            if (parser.getProcedureTypeCode() == null ) {
                TransformWarnings.log(LOG,csvHelper,"");
                return;
            }
            stagingPROCE.setProcedureType(parser.getProcedureTypeCode().getString());
            String codeId = csvHelper.getProcedureOrDiagnosisConceptCode(parser.getConceptCodeIdentifier());
            if (codeId == null) {
                TransformWarnings.log(LOG,csvHelper,"PROCE record {} has no procedure Code", procId );
                return;
            }
            stagingPROCE.setProcedureCode(codeId);
            String procTerm = TerminologyService.lookupSnomedTerm(codeId);
            if (procTerm == null) {
                TransformWarnings.log(LOG,csvHelper,"PROCE record {} has no procedure term", procId );
                return;
            }
            stagingPROCE.setProcedureTerm(procTerm);
            stagingPROCE.setProcedureSeqNo(parser.getCDSSequence().getInt());
            String personId = csvHelper.findPersonIdFromEncounterId(parser.getEncounterId());
            stagingPROCE.setLookupPersonId(Integer.parseInt(personId));
            //TYPE_MILLENNIUM_PERSON_ID_TO_MRN
            String mrn = csvHelper.getInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personId);
            if (mrn == null) {
                TransformWarnings.log(LOG,csvHelper,"PROCE record {} has no MRN from lookup", procId );
                return;
            }
            stagingPROCE.setLookupMrn(mrn);


            stagingPROCE.setCheckSum(stagingPROCE.hashCode());
            stagingPROCE.setLookupNhsNumber("0");
            stagingPROCE.setLookupDateOfBirth(new Date());
        }
        //TODO lookup_nhs and lookup_dob - how from enc?

        csvHelper.submitToThreadPool(new PROCEPreTransformer.saveDataCallable(parser.getCurrentState(), stagingPROCE, serviceId));
    }

    private static class saveDataCallable extends AbstractCsvCallable {

        private StagingPROCE obj = null;
        private UUID serviceId;

        public saveDataCallable(CsvCurrentState parserState,
                                StagingPROCE obj,
                                UUID serviceId) {
            super(parserState);
            this.obj = obj;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.save(obj, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}


