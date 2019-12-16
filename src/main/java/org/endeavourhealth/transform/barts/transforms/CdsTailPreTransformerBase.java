package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingCdsTailDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCdsTail;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingConditionCdsTail;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedureCdsTail;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.CdsTailRecordI;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.TransformConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class CdsTailPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(CdsTailPreTransformerBase.class);

    private static StagingCdsTailDalI repository = DalProvider.factoryStagingCdsTailDalI();

    protected static void processTailRecord(CdsTailRecordI parser, BartsCsvHelper csvHelper, String susRecordType,
                                            List<StagingProcedureCdsTail> procedureBatch,
                                            List<StagingConditionCdsTail> conditionBatch,
                                            List<StagingCdsTail> cdsTailBatch) throws Exception {

        if (TransformConfig.instance().isLive()) {
            processTailRecordCondition(parser, csvHelper, susRecordType, conditionBatch);
            processTailRecordProcedure(parser, csvHelper, susRecordType, procedureBatch);

        } else {
            //on Cerner Transform server, just run the latest tail records for now
            processCdsTailRecord(parser, csvHelper, susRecordType, cdsTailBatch);
            //processTailRecordCondition(parser, csvHelper, susRecordType, conditionBatch);
            //processTailRecordProcedure(parser, csvHelper, susRecordType, procedureBatch);
        }
    }

    protected static void processEmergencyCdsTailRecords(CdsTailRecordI parser, BartsCsvHelper csvHelper,
                                                   List<StagingCdsTail> cdsTailBatch) throws Exception {

        if (TransformConfig.instance().isLive()) {

            //move function to here for go live
        } else {
            processCdsTailRecord(parser, csvHelper, BartsCsvHelper.SUS_RECORD_TYPE_EMERGENCY_CDS, cdsTailBatch);
        }
    }

    protected static void saveCdsTailBatch(List<StagingCdsTail> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new CdsTailPreTransformerBase.SaveCdsTailDataCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    protected static void saveProcedureBatch(List<StagingProcedureCdsTail> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new CdsTailPreTransformerBase.SaveProcedureDataCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    protected static void saveConditionBatch(List<StagingConditionCdsTail> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new CdsTailPreTransformerBase.SaveConditionDataCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    private static void processCdsTailRecord(CdsTailRecordI parser, BartsCsvHelper csvHelper, String susRecordType, List<StagingCdsTail> cdsTailBatch) throws Exception {

        CsvCell personIdCell = parser.getPersonId();
        String personId = personIdCell.getString();
        if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
            return;
        }

        StagingCdsTail stagingCdsTailObj = new StagingCdsTail();

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(personIdCell.getPublishedFileId(), personIdCell.getRecordNumber());
        stagingCdsTailObj.setAudit(audit);

        stagingCdsTailObj.setPersonId(personIdCell.getInt());
        stagingCdsTailObj.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
        stagingCdsTailObj.setExchangeId(csvHelper.getExchangeId().toString());
        stagingCdsTailObj.setDtReceived(csvHelper.getDataDate());
        stagingCdsTailObj.setSusRecordType(susRecordType);
        stagingCdsTailObj.setCdsUpdateType(parser.getCdsUpdateType().getInt());
        stagingCdsTailObj.setMrn(parser.getLocalPatientId().getString());
        stagingCdsTailObj.setNhsNumber(parser.getNhsNumber().getString());

        stagingCdsTailObj.setEncounterId(parser.getEncounterId().getInt());

        //episodeId is not always present
        if (!parser.getEpisodeId().isEmpty()) {
            stagingCdsTailObj.setEpisodeId(parser.getEpisodeId().getInt());
        }
        stagingCdsTailObj.setResponsibleHcpPersonnelId(parser.getResponsiblePersonnelId().getInt());
        stagingCdsTailObj.setTreatmentFunctionCode(parser.getTreatmentFunctionCd().getString());

        cdsTailBatch.add(stagingCdsTailObj);
        saveCdsTailBatch(cdsTailBatch, false, csvHelper);
    }

    private static void processTailRecordProcedure(CdsTailRecordI parser, BartsCsvHelper csvHelper, String susRecordType, List<StagingProcedureCdsTail> procedureBatch) throws Exception {

        CsvCell personIdCell = parser.getPersonId();
        String personId = personIdCell.getString();
        if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
            return;
        }

        StagingProcedureCdsTail stagingObj = new StagingProcedureCdsTail();

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

        procedureBatch.add(stagingObj);
        saveProcedureBatch(procedureBatch, false, csvHelper);
    }

    private static void processTailRecordCondition(CdsTailRecordI parser, BartsCsvHelper csvHelper, String susRecordType, List<StagingConditionCdsTail> conditionBatch) throws Exception {


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

        conditionBatch.add(staging);
        saveConditionBatch(conditionBatch, false, csvHelper);
    }

    private static class SaveCdsTailDataCallable implements Callable {

        private List<StagingCdsTail> objs = null;
        private UUID serviceId;

        public SaveCdsTailDataCallable(List<StagingCdsTail> objs, UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveCdsTails(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

    private static class SaveProcedureDataCallable implements Callable {

        private List<StagingProcedureCdsTail> objs = null;
        private UUID serviceId;

        public SaveProcedureDataCallable(List<StagingProcedureCdsTail> objs, UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveProcedureTails(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

    private static class SaveConditionDataCallable implements Callable {
        //Try to abstract these 2 saves. We need the class know as save is overloaded
        private List<StagingConditionCdsTail> objs = null;
        private UUID serviceId;

        public SaveConditionDataCallable(List<StagingConditionCdsTail> objs,
                                         UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveConditionTails(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}
