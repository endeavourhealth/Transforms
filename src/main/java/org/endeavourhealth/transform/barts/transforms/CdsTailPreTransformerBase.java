package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingCdsTailDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingConditionCdsTail;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedureCdsTail;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.CdsTailRecordI;
import org.endeavourhealth.transform.common.AbstractCsvCallable;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.TransformConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class CdsTailPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(CdsTailPreTransformerBase.class);

    private static StagingCdsTailDalI repository = DalProvider.factoryStagingCdsTailDalI();

    private static Date dProceduresEnd = null;
    private static Date dProceduresStart = null;

    protected static void processTailRecord(CdsTailRecordI parser, BartsCsvHelper csvHelper, String susRecordType,
                                            List<StagingProcedureCdsTail> procedureBatch,
                                            List<StagingConditionCdsTail> conditionBatch) throws Exception {

        if (TransformConfig.instance().isLive()) {

            processTailRecordCondition(parser, csvHelper, susRecordType, conditionBatch);

            //on live, we've already processed the procedure data from 01/01/2019 to 19/06/2019 inclusive,
            //so skip them while we process the condition/diagnosis data
            Date dData = csvHelper.getDataDate();
            if (dProceduresStart == null) {
                dProceduresStart = new SimpleDateFormat("yyyy-MM-dd").parse("2019-01-01");
                dProceduresEnd = new SimpleDateFormat("yyyy-MM-dd").parse("2019-06-19");
            }
            if (dData.before(dProceduresStart)) {
                throw new Exception("Trying to run past procedures data when code to skip 2019 still in place");
            }
            if (dData.after(dProceduresEnd)) {
                processTailRecordProcedure(parser, csvHelper, susRecordType, procedureBatch);
            }

        } else {
            //on Cerner Transform server, just run diagnoses for now
            processTailRecordCondition(parser, csvHelper, susRecordType, conditionBatch);
            //processTailRecordProcedure(parser, csvHelper, susRecordType, procedureBatch);
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
