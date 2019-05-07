package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingCdsTailDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCdsTail;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.CdsTailRecordI;
import org.endeavourhealth.transform.common.AbstractCsvCallable;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class CdsTailPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(CdsTailPreTransformerBase.class);

    private static StagingCdsTailDalI repository = DalProvider.factoryStagingCdsTailDalI();

    protected static void processTailRecord(CdsTailRecordI parser, BartsCsvHelper csvHelper, String susRecordType) throws Exception{

        StagingCdsTail staging = new StagingCdsTail();
        String personId = parser.getPersonId().getString();
        if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
            return;
        }
        staging.setPersonId(parser.getPersonId().getInt());
        staging.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
        staging.setExchangeId(csvHelper.getExchangeId().toString());
        staging.setDtReceived(new Date());
        staging.setSusRecordType(susRecordType);
        staging.setCdsUpdateType(parser.getCdsUpdateType().getInt());
        staging.setMrn(parser.getLocalPatientId().getString());
        staging.setNhsNumber(parser.getNhsNumber().getString());

        staging.setEncounterId(parser.getEncounterId().getInt());
        staging.setResponsibleHcpPersonnelId(parser.getResponsiblePersonnelId().getInt());


        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveDataCallable(parser.getCurrentState(), staging, serviceId));

    }


    private static class SaveDataCallable extends AbstractCsvCallable {

        private StagingCdsTail obj = null;
        private UUID serviceId;

        public SaveDataCallable(CsvCurrentState parserState,
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
    }
}
