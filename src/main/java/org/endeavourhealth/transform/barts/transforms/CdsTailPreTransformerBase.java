package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingCdsTailDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCdsTail;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingConditionCdsTail;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.CdsTailRecordI;
import org.endeavourhealth.transform.common.AbstractCsvCallable;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.TransformConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class CdsTailPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(CdsTailPreTransformerBase.class);

    private static StagingCdsTailDalI repository = DalProvider.factoryStagingCdsTailDalI();

    protected static void processTailRecord(CdsTailRecordI parser, BartsCsvHelper csvHelper, String susRecordType) throws Exception {

        processTailRecordProcedure(parser,csvHelper,susRecordType);

        if (!TransformConfig.instance().isLive()) {
            processTailRecordCondition(parser, csvHelper, susRecordType);
        }
    }

    private static void processTailRecordProcedure(CdsTailRecordI parser, BartsCsvHelper csvHelper, String susRecordType) throws Exception{

        CsvCell personIdCell = parser.getPersonId();
        String personId = personIdCell.getString();
        if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
            return;
        }

        StagingCdsTail stagingObj = new StagingCdsTail();

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(personIdCell.getPublishedFileId(), personIdCell.getRecordNumber());
        stagingObj.setAudit(audit);

        stagingObj.setPersonId(personIdCell.getInt());
        stagingObj.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
        stagingObj.setExchangeId(csvHelper.getExchangeId().toString());
        stagingObj.setDtReceived(csvHelper.getDataDate());
        stagingObj.setSusRecordType(susRecordType);
        stagingObj.setCdsUpdateType(parser.getCdsUpdateType().getInt());
        stagingObj.setMrn(parser.getLocalPatientId().getString());
        stagingObj.setNhsNumber(parser.getNhsNumber().getString());

        stagingObj.setEncounterId(parser.getEncounterId().getInt());
        stagingObj.setResponsibleHcpPersonnelId(parser.getResponsiblePersonnelId().getInt());

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveProcedureDataCallable(parser.getCurrentState(), stagingObj, serviceId));
    }

    private static void processTailRecordCondition(CdsTailRecordI parser, BartsCsvHelper csvHelper, String susRecordType) throws Exception{


        StagingConditionCdsTail staging = new StagingConditionCdsTail();
        String personId = parser.getPersonId().getString();
        if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
            return;
        }
        staging.setPersonId(parser.getPersonId().getInt());
        staging.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
        staging.setExchangeId(csvHelper.getExchangeId().toString());
        staging.setDtReceived(csvHelper.getDataDate());
        staging.setSusRecordType(susRecordType);
        staging.setCdsUpdateType(parser.getCdsUpdateType().getInt());
        staging.setMrn(parser.getLocalPatientId().getString());
        staging.setNhsNumber(parser.getNhsNumber().getString());

        staging.setEncounterId(parser.getEncounterId().getInt());
        staging.setResponsibleHcpPersonnelId(parser.getResponsiblePersonnelId().getInt());


        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveConditionDataCallable(parser.getCurrentState(), staging, serviceId));

    }

    private static class SaveProcedureDataCallable extends AbstractCsvCallable {

        private StagingCdsTail obj = null;
        private UUID serviceId;

        public SaveProcedureDataCallable(CsvCurrentState parserState,
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
    private static class SaveConditionDataCallable extends AbstractCsvCallable {
        //Try to abstract these 2 saves. We need the class know as save is overloaded
        private StagingConditionCdsTail obj = null;
        private UUID serviceId;

        public SaveConditionDataCallable(CsvCurrentState parserState,
                                         StagingConditionCdsTail obj,
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
