package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingCdsTailDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCdsTail;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.SusPatientTailCache;
import org.endeavourhealth.transform.barts.schema.SusOutpatientTail;
import org.endeavourhealth.transform.common.AbstractCsvCallable;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class SusOutpatientTailPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SusPatientTailCache.class);
    private static StagingCdsTailDalI repository = DalProvider.factoryStagingCdsTailDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {
        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((org.endeavourhealth.transform.barts.schema.SusOutpatientTail) parser, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    private static void processRecord(SusOutpatientTail parser, BartsCsvHelper csvHelper) throws Exception{


        StagingCdsTail staging = new StagingCdsTail();
        staging.setCdsUniqueIdentifierm(parser.getCdsUniqueId().getString());
        staging.setExchangeId(parser.getExchangeId().toString());
        staging.setDTReceived(new Date());
        staging.setSusRecordType(csvHelper.SUS_RECORD_TYPE_OUTPATIENT);
        staging.setCdsUniqueIdentifierm(parser.getCdsUniqueId().getString());
        staging.setCdsUpdateType(parser.getCdsUpdateType().getInt());
        staging.setMrn(parser.getLocalPatientId().getString());
        staging.setNhsNumber(parser.getNhsNumber().getString());
        staging.setPersonId(parser.getPersonId().getInt());
        staging.setEncounterId(parser.getEncounterId().getInt());
        staging.setResponsibleHcpPersonnelId(parser.getResponsiblePersonnelId().getInt());


        UUID serviceId = csvHelper.getServiceId();
        staging.setRecordChecksum(staging.hashCode());
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
                repository.save(obj, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}


