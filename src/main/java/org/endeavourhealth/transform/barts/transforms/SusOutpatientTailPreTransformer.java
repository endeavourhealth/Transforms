package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.SusOutpatientTail;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SusOutpatientTailPreTransformer extends CdsTailPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(SusOutpatientTailPreTransformer.class);
    //private static StagingCdsTailDalI repository = DalProvider.factoryStagingCdsTailDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {
        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                //no try/catch here, since any failure here means we don't want to continue
                processTailRecord((SusOutpatientTail)parser, csvHelper, BartsCsvHelper.SUS_RECORD_TYPE_OUTPATIENT);
            }
        }
    }

    /*private static void processRecord(SusOutpatientTail parser, BartsCsvHelper csvHelper) throws Exception{

        String personId = parser.getPersonId().getString();
        if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
            return;
        }
        StagingCdsTail staging = new StagingCdsTail();
        staging.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
        staging.setExchangeId(parser.getExchangeId().toString());
        staging.setDtReceived(csvHelper.getDataDate());
        staging.setSusRecordType(BartsCsvHelper.SUS_RECORD_TYPE_OUTPATIENT);
        staging.setCdsUpdateType(parser.getCdsUpdateType().getInt());
        staging.setMrn(parser.getLocalPatientId().getString());
        staging.setNhsNumber(parser.getNhsNumber().getString());
        staging.setPersonId(parser.getPersonId().getInt());
        staging.setEncounterId(parser.getEncounterId().getInt());
        staging.setResponsibleHcpPersonnelId(parser.getResponsiblePersonnelId().getInt());

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SusOutpatientTailPreTransformer.saveDataCallable(parser.getCurrentState(), staging, serviceId));

    }


    private static class saveDataCallable extends AbstractCsvCallable {

        private StagingCdsTail obj = null;
        private UUID serviceId;

        public saveDataCallable(CsvCurrentState parserState,
                                StagingCdsTail obj,
                                UUID serviceId) {
            super(parserState);
            this.obj = obj;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                obj.setRecordChecksum(obj.hashCode());
                repository.save(obj, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }*/
}


