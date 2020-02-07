package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingCdsDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.*;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsSusHelper;
import org.endeavourhealth.transform.barts.schema.*;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

public abstract class CdsPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(CdsPreTransformerBase.class);

    private static StagingCdsDalI repository = DalProvider.factoryStagingCdsDalI();
    private final static String BARTS_UNKNOWN_OPCS_CODE_Y926 = "Y92.6";
    private final static String BARTS_UNKNOWN_ICD10_CODE_Z669 = "Z66.9";
    protected static void processRecords(CdsRecordI parser, BartsCsvHelper csvHelper, String susRecordType,
                                         List<StagingProcedureCds> procedureBatch,
                                         List<StagingProcedureCdsCount> procedureCountBatch,
                                         List<StagingConditionCds> conditionBatch,
                                         List<StagingConditionCdsCount> conditionCountBatch) throws Exception {

        if (TransformConfig.instance().isLive()) {
            processDiagnoses(parser, csvHelper, susRecordType, conditionBatch, conditionCountBatch);
            processProcedures(parser, csvHelper, susRecordType, procedureBatch, procedureCountBatch);

        } else {

            //processDiagnoses(parser, csvHelper, susRecordType, conditionBatch, conditionCountBatch);
            //processProcedures(parser, csvHelper, susRecordType, procedureBatch, procedureCountBatch);
        }
    }

    protected static void processInpatientRecords(CdsRecordInpatientI parser, BartsCsvHelper csvHelper,
                                         List<StagingInpatientCds> inpatientCdsBatch) throws Exception {

        if (TransformConfig.instance().isLive()) {

            //move function to here for go live
        } else {
            processInpatients(parser, csvHelper, inpatientCdsBatch);
        }
    }

    protected static void processOutpatientRecords(CdsRecordOutpatientI parser, BartsCsvHelper csvHelper,
                                         List<StagingOutpatientCds> outpatientCdsBatch) throws Exception {

        if (TransformConfig.instance().isLive()) {

            //move function to here for go live
        } else {
            processOutpatients(parser, csvHelper, outpatientCdsBatch);
        }
    }

    protected static void processEmergencyCdsRecords(CdsRecordEmergencyCDSI parser, BartsCsvHelper csvHelper,
                                         List<StagingEmergencyCds> emergencyCdsBatch) throws Exception {

        if (TransformConfig.instance().isLive()) {

            //move function to here for go live
        } else {
            processEmergencies(parser, csvHelper, emergencyCdsBatch);
        }
    }

    protected static void saveEmergencyCdsBatch(List<StagingEmergencyCds> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsEmergencyCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    protected static void processCriticalCareCdsRecords(CdsRecordCriticalCareI parser, BartsCsvHelper csvHelper,
                                                        List<StagingCriticalCareCds> criticalCareCdsBatch) throws Exception {

        if (TransformConfig.instance().isLive()) {

            //move function to here for go live
        } else {
            processCriticalCares(parser, csvHelper, criticalCareCdsBatch);
        }
    }

    protected static void saveCriticalCareCdsBatch(List<StagingCriticalCareCds> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsCriticalCareCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    protected static void processHomeDelBirthCdsRecords(CdsRecordInpatientI parser, BartsCsvHelper csvHelper,
                                                        List<StagingHomeDelBirthCds> homeDelBirthCdsBatch) throws Exception {

        if (TransformConfig.instance().isLive()) {

            //move function to here for go live
        } else {
            processHomeDelBirths(parser, csvHelper, homeDelBirthCdsBatch);
        }
    }

    protected static void saveHomeDelBirthCdsBatch(List<StagingHomeDelBirthCds> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsHomeDelBirthCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }


    protected static void saveOutpatientCdsBatch(List<StagingOutpatientCds> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsOutpatientCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    protected static void saveInpatientCdsBatch(List<StagingInpatientCds> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsInpatientCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    protected static void saveProcedureBatch(List<StagingProcedureCds> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsProcedureCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    protected static void saveConditionBatch(List<StagingConditionCds> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsConditionCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    protected static void saveProcedureCountBatch(List<StagingProcedureCdsCount> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsProcedureCountCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    protected static void saveConditionCountBatch(List<StagingConditionCdsCount> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsConditionCountCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }


    private static void processProcedures(CdsRecordI parser, BartsCsvHelper csvHelper, String susRecordType,
                                          List<StagingProcedureCds> procedureBatch,
                                          List<StagingProcedureCdsCount> procedureCountBatch) throws Exception {

        StagingProcedureCds stagingCds = new StagingProcedureCds();

        CsvCell cdsUniqueIdCell = parser.getCdsUniqueId();
        stagingCds.setCdsUniqueIdentifier(cdsUniqueIdCell.getString());

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(cdsUniqueIdCell.getPublishedFileId(), cdsUniqueIdCell.getRecordNumber());
        stagingCds.setAudit(audit);

        stagingCds.setExchangeId(csvHelper.getExchangeId().toString());
        stagingCds.setDtReceived(csvHelper.getDataDate());
        stagingCds.setCdsActivityDate(parser.getCdsActivityDate().getDate());
        stagingCds.setSusRecordType(susRecordType);
        stagingCds.setCdsUpdateType(parser.getCdsUpdateType().getInt());

        CsvCell withheldCell = parser.getWithheldFlag();
        boolean isWithheld = withheldCell.getBoolean();
        stagingCds.setWithheld(new Boolean(isWithheld));

        if (!isWithheld) { //LocalPatientId and NHS number should be empty if withheld. Get persondId from tail file
            String localPatientId = parser.getLocalPatientId().getString();
            stagingCds.setMrn(localPatientId);
            stagingCds.setNhsNumber(parser.getNhsNumber().getString());
            String personId = csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, localPatientId);
            if (Strings.isNullOrEmpty(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Failed to find personid for CDS id {}", parser.getCdsUniqueId());
                return;
            }
            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Skipping CDS record {} as not part of filtered subset", parser.getCdsUniqueId());
                return;
            }
            stagingCds.setLookupPersonId(Integer.valueOf(personId));
        }
        stagingCds.setDateOfBirth(parser.getPersonBirthDate().getDate());
        String consultantStr = parser.getConsultantCode().getString();
        stagingCds.setConsultantCode(consultantStr);

        String personnelIdStr = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_CONSULTANT_TO_ID, consultantStr);
        if (!Strings.isNullOrEmpty(personnelIdStr)) {
            stagingCds.setLookupConsultantPersonnelId(Integer.valueOf(personnelIdStr));
        }

        int procedureCount = 0;

        //primary procedure
        if (parsePrimaryProcedure(parser, stagingCds, csvHelper, procedureBatch)) {
            procedureCount++;
        }

        //Secondary
        if (parseSecondaryProcedure(parser, stagingCds, csvHelper, procedureBatch)) {
            procedureCount++;
        }

        //Rest
        procedureCount += parseRemainingProcedures(parser, stagingCds, csvHelper, procedureBatch);

        //persist the count of procedures in this cds record
        StagingProcedureCdsCount stagingCdsCount = new StagingProcedureCdsCount();
        stagingCdsCount.setExchangeId(csvHelper.getExchangeId().toString());
        stagingCdsCount.setDtReceived(csvHelper.getDataDate());
        stagingCdsCount.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
        stagingCdsCount.setSusRecordType(susRecordType);
        stagingCdsCount.setProcedureCount(procedureCount);

        //audit that our staging object came from this file and record
        audit = new ResourceFieldMappingAudit();
        audit.auditRecord(cdsUniqueIdCell.getPublishedFileId(), cdsUniqueIdCell.getRecordNumber());
        stagingCdsCount.setAudit(audit);

        procedureCountBatch.add(stagingCdsCount);
        saveProcedureCountBatch(procedureCountBatch, false, csvHelper);
    }

    private static boolean parsePrimaryProcedure(CdsRecordI parser, StagingProcedureCds commonContent, BartsCsvHelper csvHelper, List<StagingProcedureCds> procedureBatch) throws Exception {

        //if no procedures, then nothing to save
        CsvCell primaryProcedureCell = parser.getPrimaryProcedureOPCS();
        if (primaryProcedureCell.isEmpty()) {
            return false;
        }

        StagingProcedureCds cdsPrimary = commonContent.clone();

        String opcsCode = primaryProcedureCell.getString();
        opcsCode = TerminologyService.standardiseOpcs4Code(opcsCode);
        cdsPrimary.setProcedureOpcsCode(opcsCode);

        String term = TerminologyService.lookupOpcs4ProcedureName(opcsCode);
        if (Strings.isNullOrEmpty(term)) {
            throw new Exception("Failed to find term for OPCS-4 code " + opcsCode);
        }
        cdsPrimary.setLookupProcedureOpcsTerm(term);

        cdsPrimary.setProcedureSeqNbr(1);

        CsvCell dateCell = parser.getPrimaryProcedureDate();
        //date is null in some cases, but that's fine, as the SQL SP will fall back on CDS activity date
        if (!dateCell.isEmpty()) {
            cdsPrimary.setProcedureDate(dateCell.getDate());
        }

        procedureBatch.add(cdsPrimary);
        saveProcedureBatch(procedureBatch, false, csvHelper);

        //for secondary etc. we set the primary opcs code on a separate column so set on the common object
        commonContent.setPrimaryProcedureOpcsCode(opcsCode);

        return true;
    }

    private static boolean parseSecondaryProcedure(CdsRecordI parser, StagingProcedureCds commonContent, BartsCsvHelper csvHelper, List<StagingProcedureCds> procedureBatch) throws Exception {
        CsvCell secondaryProcedureCell = parser.getSecondaryProcedureOPCS();
        if (secondaryProcedureCell.isEmpty()) {
            //if no secondary procedure, then we're finished
            return false;
        }

        StagingProcedureCds cdsSecondary = commonContent.clone();

        String opcsCode = secondaryProcedureCell.getString();
        opcsCode = TerminologyService.standardiseOpcs4Code(opcsCode);
        cdsSecondary.setProcedureOpcsCode(opcsCode);

        String term = TerminologyService.lookupOpcs4ProcedureName(opcsCode);
        if (Strings.isNullOrEmpty(term)) {
            throw new Exception("Failed to find term for OPCS-4 code " + opcsCode);
        }
        cdsSecondary.setLookupProcedureOpcsTerm(term);
        cdsSecondary.setProcedureSeqNbr(2);

        CsvCell dateCell = parser.getSecondaryProcedureDate();
        if (!dateCell.isEmpty()) {
            cdsSecondary.setProcedureDate(dateCell.getDate());
        } else {
            //if we have no secondary date, use the primary date
            CsvCell primaryDateCell = parser.getPrimaryProcedureDate();
            if (!primaryDateCell.isEmpty()) {
                cdsSecondary.setProcedureDate(primaryDateCell.getDate());
            }
        }

        procedureBatch.add(cdsSecondary);
        saveProcedureBatch(procedureBatch, false, csvHelper);

        return true;
    }

    private static int parseRemainingProcedures(CdsRecordI parser, StagingProcedureCds commonContent, BartsCsvHelper csvHelper, List<StagingProcedureCds> procedureBatch) throws Exception {
        CsvCell otherProcedureOPCS = parser.getAdditionalSecondaryProceduresOPCS();
        if (otherProcedureOPCS.isEmpty()) {
            return 0;
        }

        int remainingProcedureCount = 0;
        int seq = 3;

        for (String word : BartsSusHelper.splitEqually(otherProcedureOPCS.getString(), 40)) {
            if (Strings.isNullOrEmpty(word)) {
                break;
            }
            StagingProcedureCds cdsRemainder = commonContent.clone();

            String opcsCode = word.substring(0, 4);
            opcsCode = opcsCode.trim(); //because we sometimes get just three chars e.g. S41
            if (opcsCode.isEmpty()) {
                break;
            }
            opcsCode = TerminologyService.standardiseOpcs4Code(opcsCode);
            cdsRemainder.setProcedureOpcsCode(opcsCode);

            String dateStr = null;
            if (word.length() > 4) {
                dateStr = word.substring(4, 12);
                dateStr = dateStr.trim();
            }

            if (!Strings.isNullOrEmpty(dateStr)) {
                Date date = parser.getDateFormat().parse(dateStr);
                cdsRemainder.setProcedureDate(date);
            } else {
                //if we have no secondary date, use the primary date
                CsvCell dateCell = parser.getPrimaryProcedureDate();
                if (!dateCell.isEmpty()) {
                    cdsRemainder.setProcedureDate(dateCell.getDate());
                }
            }

            String term = TerminologyService.lookupOpcs4ProcedureName(opcsCode);
            if (Strings.isNullOrEmpty(term)) {
                //if (opcsCode.equals(BARTS_UNKNOWN_OPCS_CODE_Y926)) {
                //    term = BARTS_UNKNOWN_OPCS_CODE_Y926;
                //    TransformWarnings.log(LOG, csvHelper,"Undefined OPCS code from Barts {}",BARTS_UNKNOWN_OPCS_CODE_Y926);
                //} else {
                //    throw new Exception("Failed to find term for OPCS-4 code " + opcsCode);
                //}
                TransformWarnings.log(LOG, csvHelper,"Undefined OPCS code from Barts {}",BARTS_UNKNOWN_OPCS_CODE_Y926);
                term = "Barts undefined OPCS code " + opcsCode;
            }
            cdsRemainder.setLookupProcedureOpcsTerm(term);

            cdsRemainder.setProcedureSeqNbr(seq);

            procedureBatch.add(cdsRemainder);
            saveProcedureBatch(procedureBatch, false, csvHelper);

            seq++;
            remainingProcedureCount++;
        }

        return remainingProcedureCount;
    }

    private static class SaveCdsHomeDelBirthCallable implements Callable {

        private List<StagingHomeDelBirthCds> objs = null;
        private UUID serviceId;

        public SaveCdsHomeDelBirthCallable(List<StagingHomeDelBirthCds> objs, UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveCDSHomeDelBirths(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

    private static class SaveCdsCriticalCareCallable implements Callable {

        private List<StagingCriticalCareCds> objs = null;
        private UUID serviceId;

        public SaveCdsCriticalCareCallable(List<StagingCriticalCareCds> objs, UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveCDSCriticalCares(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

    private static class SaveCdsEmergencyCallable implements Callable {

        private List<StagingEmergencyCds> objs = null;
        private UUID serviceId;

        public SaveCdsEmergencyCallable(List<StagingEmergencyCds> objs, UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveCDSEmergencies(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

    private static class SaveCdsOutpatientCallable implements Callable {

        private List<StagingOutpatientCds> objs = null;
        private UUID serviceId;

        public SaveCdsOutpatientCallable(List<StagingOutpatientCds> objs, UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveCDSOutpatients(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

    private static class SaveCdsInpatientCallable implements Callable {

        private List<StagingInpatientCds> objs = null;
        private UUID serviceId;

        public SaveCdsInpatientCallable(List<StagingInpatientCds> objs, UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveCDSInpatients(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

    private static class SaveCdsProcedureCallable implements Callable {

        private List<StagingProcedureCds> objs = null;
        private UUID serviceId;

        public SaveCdsProcedureCallable(List<StagingProcedureCds> objs, UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveProcedures(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

    private static class SaveCdsProcedureCountCallable implements Callable {

        private List<StagingProcedureCdsCount> objs = null;
        private UUID serviceId;

        public SaveCdsProcedureCountCallable(List<StagingProcedureCdsCount> objs, UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveProcedureCounts(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

    private static void processDiagnoses(CdsRecordI parser, BartsCsvHelper csvHelper, String susRecordType,
                                         List<StagingConditionCds> conditionBatch,
                                         List<StagingConditionCdsCount> conditionCountBatch) throws Exception {

        StagingConditionCds stagingConditionCds = new StagingConditionCds();

        CsvCell cdsUniqueIdCell = parser.getCdsUniqueId();
        stagingConditionCds.setCdsUniqueIdentifier(cdsUniqueIdCell.getString());

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(cdsUniqueIdCell.getPublishedFileId(), cdsUniqueIdCell.getRecordNumber());
        stagingConditionCds.setAudit(audit);

        stagingConditionCds.setExchangeId(csvHelper.getExchangeId().toString());
        stagingConditionCds.setDtReceived(csvHelper.getDataDate());
        stagingConditionCds.setCdsActivityDate(parser.getCdsActivityDate().getDate());
        stagingConditionCds.setSusRecordType(susRecordType);
        stagingConditionCds.setCdsUpdateType(parser.getCdsUpdateType().getInt());

        CsvCell withheldCell = parser.getWithheldFlag();
        boolean isWithheld = withheldCell.getBoolean();
        stagingConditionCds.setWithheld(new Boolean(isWithheld));

        if (!isWithheld) { //LocalPatientId and NHS number should be empty if withheld. Get persondId from tail file
            String localPatientId = parser.getLocalPatientId().getString();
            stagingConditionCds.setMrn(localPatientId);
            stagingConditionCds.setNhsNumber(parser.getNhsNumber().getString());
            String personId = csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, localPatientId);
            if (Strings.isNullOrEmpty(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Failed to find personid for CDS id {}", parser.getCdsUniqueId());
                return;
            }
            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Skipping CDS record {} as not part of filtered subset", parser.getCdsUniqueId());
                return;
            }
            stagingConditionCds.setLookupPersonId(Integer.valueOf(personId));
        }
        stagingConditionCds.setDateOfBirth(parser.getPersonBirthDate().getDate());
        String consultantStr = parser.getConsultantCode().getString();
        stagingConditionCds.setConsultantCode(consultantStr);

        String personnelIdStr = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_CONSULTANT_TO_ID, consultantStr);
        if (!Strings.isNullOrEmpty(personnelIdStr)) {
            stagingConditionCds.setLookupConsultantPersonnelId(Integer.valueOf(personnelIdStr));
        }

        int conditionCount = 0;

        //primary Diagnosis
        if (parsePrimaryDiagnosis(parser, stagingConditionCds, csvHelper, conditionBatch)) {
            conditionCount++;
        }

        //Secondary
        if (parseSecondaryDiagnosis(parser, stagingConditionCds, csvHelper, conditionBatch)) {
            conditionCount++;
        }

        //Rest
        conditionCount += parseRemainingDiagnoses(parser, stagingConditionCds, csvHelper, conditionBatch);

        //persist the count of conditions in this cds record
        StagingConditionCdsCount stagingConditionCdsCount = new StagingConditionCdsCount();
        stagingConditionCdsCount.setExchangeId(csvHelper.getExchangeId().toString());
        stagingConditionCdsCount.setDtReceived(csvHelper.getDataDate());
        stagingConditionCdsCount.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
        stagingConditionCdsCount.setSusRecordType(susRecordType);
        stagingConditionCdsCount.setConditionCount(conditionCount);

        //audit that our staging object came from this file and record
        audit = new ResourceFieldMappingAudit();
        audit.auditRecord(cdsUniqueIdCell.getPublishedFileId(), cdsUniqueIdCell.getRecordNumber());
        stagingConditionCds.setAudit(audit);

        conditionCountBatch.add(stagingConditionCdsCount);
        saveConditionCountBatch(conditionCountBatch, false, csvHelper);
    }


    private static boolean parsePrimaryDiagnosis(CdsRecordI parser, StagingConditionCds commonContent, BartsCsvHelper csvHelper, List<StagingConditionCds> conditionBatch) throws Exception {

        //if no diagnoses, then nothing to save
        CsvCell primaryDiagnosisCell = parser.getPrimaryDiagnosisICD();
        if (primaryDiagnosisCell.isEmpty()) {
            return false;
        }

        StagingConditionCds cdsPrimary = commonContent.clone();

        String icdCode = primaryDiagnosisCell.getString().trim();

        if (icdCode.length() > 4 && icdCode.indexOf(".") < 0) {
            //TransformWarnings.log(LOG, csvHelper, "Long code found. Shortening : {}", icdCode);
            icdCode = icdCode.substring(0, 4);
        }

        icdCode = TerminologyService.standardiseIcd10Code(icdCode);
        cdsPrimary.setDiagnosisIcdCode(icdCode);
 /*
            ".x" or ".X" is a placeholder in ICD-10.  Our lookup will just have the first part so remove the placeholder.
            https://www.cms.gov/Medicare/Coding/ICD10/Downloads/032310_ICD10_Slides.pdf

             */
        if (icdCode.toUpperCase().endsWith(".X")) {
            //TransformWarnings.log(LOG, csvHelper, "Truncating ICD-10 code : {}", icdCode);
            icdCode = icdCode.substring(0, 3);
        }
        String term = TerminologyService.lookupIcd10CodeDescription(icdCode);
        if (Strings.isNullOrEmpty(term)) {
            if (icdCode.equals(BARTS_UNKNOWN_ICD10_CODE_Z669)) {
                term = "Undefined ICD-10 code from Barts";
                TransformWarnings.log(LOG,csvHelper, "Undefined ICD10 code from Barts {}", icdCode);
            } else {
                throw new Exception("Failed to find term for ICD-10 code " + icdCode);
            }
        }
        cdsPrimary.setLookupDiagnosisIcdTerm(term);
        cdsPrimary.setDiagnosisSeqNbr(1);

        CsvCell dateCell = parser.getCdsActivityDate();
        //Diagnosis entries don't embed dates. All on same day as CdsActivityDate which should always be set.
        if (!dateCell.isEmpty()) {
            cdsPrimary.setCdsActivityDate(dateCell.getDate());
        }

        conditionBatch.add(cdsPrimary);
        saveConditionBatch(conditionBatch, false, csvHelper);

        //for secondary etc. we set the primary opcs code on a separate column so set on the common object
        commonContent.setPrimaryDiagnosisIcdCodee(icdCode);

        return true;
    }

    private static boolean parseSecondaryDiagnosis(CdsRecordI parser, StagingConditionCds commonContent, BartsCsvHelper csvHelper, List<StagingConditionCds> conditionBatch) throws Exception {
        CsvCell secondaryDiagnosisCell = parser.getSecondaryDiagnosisICD();
        if (secondaryDiagnosisCell.isEmpty()) {
            //if no secondary diagnosis, then we're finished
            return false;
        }

        StagingConditionCds cdsSecondary = commonContent.clone();

        String icdCode = secondaryDiagnosisCell.getString().trim();
        icdCode = TerminologyService.standardiseIcd10Code(icdCode);
        cdsSecondary.setDiagnosisIcdCode(icdCode);
         /*
            ".x" or ".X" is a placeholder in ICD-10.  Our lookup will just have the first part so remove the placeholder.
            https://www.cms.gov/Medicare/Coding/ICD10/Downloads/032310_ICD10_Slides.pdf

             */
        if (icdCode.toUpperCase().endsWith(".X")) {
            //TransformWarnings.log(LOG, csvHelper, "Truncating ICD-10 code : {}", icdCode);
            icdCode = icdCode.substring(0, 3);
        }

        String term = TerminologyService.lookupIcd10CodeDescription(icdCode);
        if (Strings.isNullOrEmpty(term)) {
            TransformWarnings.log(LOG, csvHelper, "#unmappedicd10 Unmapped ICD-10 code : {}", icdCode);
            //    throw new Exception("Failed to find term for ICD 10 code " + icdCode);
            term = "unmapped code from Barts";
        }
        cdsSecondary.setLookupDiagnosisIcdTerm(term);
        cdsSecondary.setDiagnosisSeqNbr(2);

        CsvCell dateCell = parser.getCdsActivityDate();
        if (!dateCell.isEmpty()) {
            cdsSecondary.setCdsActivityDate(dateCell.getDate());
        }

        conditionBatch.add(cdsSecondary);
        saveConditionBatch(conditionBatch, false, csvHelper);

        return true;
    }

    private static int parseRemainingDiagnoses(CdsRecordI parser, StagingConditionCds commonContent, BartsCsvHelper csvHelper, List<StagingConditionCds> conditionBatch) throws Exception {
        CsvCell otherDiagnosisICD = parser.getAdditionalSecondaryDiagnosisICD();
        if (otherDiagnosisICD.isEmpty()) {
            return 0;
        }

        int remainingConditionCount = 0;
        int seq = 3;

        for (String word : BartsSusHelper.splitEqually(otherDiagnosisICD.getString(), 7)) {
            if (Strings.isNullOrEmpty(word)) {
                break;
            }
            StagingConditionCds cdsRemainder = commonContent.clone();
            //String icdCode = word.substring(0, 4);
            // 0,4 restricts us to ICD-9 codes. We handle ICD-10 elsewhere. Up to 7
            String icdCode = word.trim();
            icdCode = icdCode.trim(); //because we sometimes get just three chars e.g. S41
            if (icdCode.isEmpty()) {
                break;
            }

            icdCode = TerminologyService.standardiseIcd10Code(icdCode);
             /*
            ".x" or ".X" is a placeholder in ICD-10.  Our lookup will just have the first part so remove the placeholder.
            https://www.cms.gov/Medicare/Coding/ICD10/Downloads/032310_ICD10_Slides.pdf

             */
            if (icdCode.toUpperCase().endsWith(".X")) {
                //TransformWarnings.log(LOG, csvHelper, "Truncating ICD-10 code : {}", icdCode);
                icdCode = icdCode.substring(0, 3);
            }
            cdsRemainder.setDiagnosisIcdCode(icdCode);

            CsvCell dateCell = parser.getCdsActivityDate();
            if (!dateCell.isEmpty()) {
                cdsRemainder.setCdsActivityDate(dateCell.getDate());
            }

            String term = TerminologyService.lookupIcd10CodeDescription(icdCode);
            if (Strings.isNullOrEmpty(term)) {
                TransformWarnings.log(LOG, csvHelper, "#unmappedicd10 Unmapped ICD-10 code : {}", icdCode);
                //throw new Exception("Failed to find term for ICD-10 code " + icdCode);
                term = "Unmapped code from Barts";
            }
            cdsRemainder.setLookupDiagnosisIcdTerm(term);

            cdsRemainder.setDiagnosisSeqNbr(seq);

            conditionBatch.add(cdsRemainder);
            saveConditionBatch(conditionBatch, false, csvHelper);

            seq++;
            remainingConditionCount++;
        }

        return remainingConditionCount;
    }

    private static class SaveCdsConditionCallable implements Callable {

        private List<StagingConditionCds> objs = null;
        private UUID serviceId;

        public SaveCdsConditionCallable(List<StagingConditionCds> objs, UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveConditions(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

    private static class SaveCdsConditionCountCallable implements Callable {

        private List<StagingConditionCdsCount> objs = null;
        private UUID serviceId;

        public SaveCdsConditionCountCallable(List<StagingConditionCdsCount> objs, UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveConditionCounts(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

    private static void processCriticalCares(CdsRecordCriticalCareI parser, BartsCsvHelper csvHelper,
                                           List<StagingCriticalCareCds> criticalCareCdsBatch) throws Exception {

        StagingCriticalCareCds stagingCriticalCareCds = new StagingCriticalCareCds();

        CsvCell cdsUniqueIdCell = parser.getCdsUniqueId();
        stagingCriticalCareCds.setCdsUniqueIdentifier(cdsUniqueIdCell.getString());

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(cdsUniqueIdCell.getPublishedFileId(), cdsUniqueIdCell.getRecordNumber());
        stagingCriticalCareCds.setAudit(audit);

        stagingCriticalCareCds.setExchangeId(csvHelper.getExchangeId().toString());
        stagingCriticalCareCds.setDtReceived(csvHelper.getDataDate());

        String localPatientId = parser.getLocalPatientId().getString();
        if (!localPatientId.isEmpty()) {

            stagingCriticalCareCds.setMrn(localPatientId);
            stagingCriticalCareCds.setNhsNumber(parser.getNhsNumber().getString());
            String personId = csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, localPatientId);
            if (Strings.isNullOrEmpty(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Failed to find personid for CDS id {}", parser.getCdsUniqueId());
                return;
            }
            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Skipping CDS record {} as not part of filtered subset", parser.getCdsUniqueId());
                return;
            }
            stagingCriticalCareCds.setLookupPersonId(Integer.valueOf(personId));

        } else {
            TransformWarnings.log(LOG, csvHelper, "Skipping Critical Care CDS record {} with no MRN", parser.getCdsUniqueId());
            return;
        }

        stagingCriticalCareCds.setCriticalCareTypeId(parser.getCriticalCareTypeID().getString());
        stagingCriticalCareCds.setSpellNumber(parser.getSpellNumber().getString());
        stagingCriticalCareCds.setEpisodeNumber(parser.getEpisodeNumber().getString());
        stagingCriticalCareCds.setCriticalCareIdentifier(parser.getCriticalCareIdentifier().getString());

        CsvCell careStartDate = parser.getCriticalCareStartDate();
        CsvCell careStartTime = parser.getCriticalCareStartTime();
        Date careDateTime = CsvCell.getDateTimeFromTwoCells(careStartDate, careStartTime);
        if (careDateTime != null) {
            stagingCriticalCareCds.setCareStartDate(careDateTime);
        }

        stagingCriticalCareCds.setCareUnitFunction(parser.getCriticalCareUnitFunction().getString());
        stagingCriticalCareCds.setAdmissionSourceCode(parser.getCriticalCareAdmissionSource().getString());
        stagingCriticalCareCds.setAdmissionTypeCode(parser.getCriticalCareAdmissionType().getString());
        stagingCriticalCareCds.setAdmissionLocation(parser.getCriticalCareSourceLocation().getString());
        stagingCriticalCareCds.setGestationLengthAtDelivery(parser.getGestationLengthAtDelivery().getString());

        stagingCriticalCareCds.setAdvancedRespiratorySupportDays(parser.getAdvancedRespiratorySupportDays().getInt());
        stagingCriticalCareCds.setBasicRespiratorySupportsDays(parser.getBasicRespiratorySupportsDays().getInt());
        stagingCriticalCareCds.setAdvancedCardiovascularSupportDays(parser.getAdvancedCardiovascularSupportDays().getInt());
        stagingCriticalCareCds.setBasicCardiovascularSupportDays(parser.getBasicCardiovascularSupportDays().getInt());
        stagingCriticalCareCds.setRenalSupportDays(parser.getRenalSupportDays().getInt());
        stagingCriticalCareCds.setNeurologicalSupportDays(parser.getNeurologicalSupportDays().getInt());
        stagingCriticalCareCds.setGastroIntestinalSupportDays(parser.getGastroIntestinalSupportDays().getInt());
        stagingCriticalCareCds.setDermatologicalSupportDays(parser.getDermatologicalSupportDays().getInt());
        stagingCriticalCareCds.setLiverSupportDays(parser.getLiverSupportDays().getInt());
        stagingCriticalCareCds.setOrganSupportMaximum(parser.getOrganSupportMaximum().getInt());
        stagingCriticalCareCds.setCriticalCareLevel2Days(parser.getCriticalCareLevel2Days().getInt());
        stagingCriticalCareCds.setCriticalCareLevel3Days(parser.getCriticalCareLevel3Days().getInt());

        CsvCell dischargeDate = parser.getCriticalCareDischargeDate();
        CsvCell dischargeTime = parser.getCriticalCareDischargeTime();
        Date dischargeDateTime = CsvCell.getDateTimeFromTwoCells(dischargeDate, dischargeTime);
        if (dischargeDateTime != null) {
            stagingCriticalCareCds.setDischargeDate(dischargeDateTime);
        }

        CsvCell dischargeReadyDate = parser.getCriticalCareDischargeReadyDate();
        CsvCell dischargeReadyTime = parser.getCriticalCareDischargeReadyTime();
        Date dischargeReadyDateTime = CsvCell.getDateTimeFromTwoCells(dischargeReadyDate, dischargeReadyTime);
        if (dischargeReadyDateTime != null) {
            stagingCriticalCareCds.setDischargeReadyDate(dischargeReadyDateTime);
        }
        stagingCriticalCareCds.setDischargeStatusCode(parser.getCriticalCareDischargeStatus().getString());
        stagingCriticalCareCds.setDischargeDestination(parser.getCriticalCareDischargeDestination().getString());
        stagingCriticalCareCds.setDischargeLocation(parser.getCriticalCareDischargeLocation().getString());

        CsvCell careActivity1Cell = parser.getCareActivity1();
        if (careActivity1Cell != null && !careActivity1Cell.isEmpty()) {
            stagingCriticalCareCds.setCareActivity1(parser.getCareActivity1().getString());
        }
        CsvCell careActivity2100Cell = parser.getCareActivity2100();
        if (careActivity2100Cell != null && !careActivity2100Cell.isEmpty()) {
            stagingCriticalCareCds.setCareActivity2100(parser.getCareActivity2100().getString());
        }

        //finally, add the Cds batch for saving
        criticalCareCdsBatch.add(stagingCriticalCareCds);
        saveCriticalCareCdsBatch(criticalCareCdsBatch, false, csvHelper);
    }

    private static void processEmergencies(CdsRecordEmergencyCDSI parser, BartsCsvHelper csvHelper,
                                           List<StagingEmergencyCds> emergencyCdsBatch) throws Exception {

        StagingEmergencyCds stagingEmergencyCds = new StagingEmergencyCds();

        CsvCell cdsUniqueIdCell = parser.getCdsUniqueId();
        stagingEmergencyCds.setCdsUniqueIdentifier(cdsUniqueIdCell.getString());

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(cdsUniqueIdCell.getPublishedFileId(), cdsUniqueIdCell.getRecordNumber());
        stagingEmergencyCds.setAudit(audit);

        stagingEmergencyCds.setExchangeId(csvHelper.getExchangeId().toString());
        stagingEmergencyCds.setDtReceived(csvHelper.getDataDate());

        //Cds activity date is common to all Sus files so if it is missing, log and exit record
        CsvCell cdsActivityDateCell = parser.getCdsActivityDate();
        if (!cdsActivityDateCell.isEmpty()) {
            stagingEmergencyCds.setCdsActivityDate(parser.getCdsActivityDate().getDate());
        } else {
            TransformWarnings.log(LOG, csvHelper, "Missing EmergencyCds cdsActivityDate CDS id {}", parser.getCdsUniqueId());
            return;
        }

        stagingEmergencyCds.setCdsUpdateType(parser.getCdsUpdateType().getInt());

        //the file only has a withheld reason. if populated, then withHeld = true
        CsvCell withheldReasonCell = parser.getWithheldReason();
        boolean isWithheld = !withheldReasonCell.isEmpty();
        stagingEmergencyCds.setWithheld(new Boolean(isWithheld));

        String localPatientId = parser.getLocalPatientId().getString();
        if (!isWithheld) { //LocalPatientId and NHS number should be empty if withheld. Get personId from internal lookup

            stagingEmergencyCds.setMrn(localPatientId);
            stagingEmergencyCds.setNhsNumber(parser.getNhsNumber().getString());
            String personId = csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, localPatientId);
            if (Strings.isNullOrEmpty(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Failed to find personid for CDS id {}", parser.getCdsUniqueId());
                return;
            }
            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Skipping CDS record {} as not part of filtered subset", parser.getCdsUniqueId());
                return;
            }
            stagingEmergencyCds.setLookupPersonId(Integer.valueOf(personId));
        } else {

            //if withheld and mrn / local patientId is null then log and return as we cannot derive patient and mrn cannot be null
            if (Strings.isNullOrEmpty(localPatientId)) {
                TransformWarnings.log(LOG, csvHelper, "Skipping withheld CDS record {} with no MRN", parser.getCdsUniqueId());
                return;
            }
        }

        CsvCell patientDob = parser.getPersonBirthDate();
        if (patientDob != null && !patientDob.isEmpty()) {
            stagingEmergencyCds.setDateOfBirth(patientDob.getDate());
        }

        stagingEmergencyCds.setPatientPathwayIdentifier(parser.getPatientPathwayIdentifier().getString());
        stagingEmergencyCds.setDepartmentType(parser.getDepartmentType().getString());
        stagingEmergencyCds.setAmbulanceIncidentNumber(parser.getAmbulanceIncidentNumber().getString());
        stagingEmergencyCds.setTreatmentOrganisationCode(parser.getTreatmentOrganisationCode().getString());
        stagingEmergencyCds.setAttendanceIdentifier(parser.getAttendanceIdentifier().getString());
        stagingEmergencyCds.setArrivalMode(parser.getArrivalMode().getString());
        stagingEmergencyCds.setAttendanceCategory(parser.getAttendanceCategory().getString());
        stagingEmergencyCds.setAttendanceSource(parser.getAttendanceSource().getString());

        CsvCell arrivalDate = parser.getArrivalDate();
        CsvCell arrivalTime = parser.getArrivalTime();
        Date arrivalDateTime = CsvCell.getDateTimeFromTwoCells(arrivalDate, arrivalTime);
        if (arrivalDateTime != null) {
            stagingEmergencyCds.setArrivalDate(arrivalDateTime);
        }
        CsvCell initialAssessmentDate = parser.getInitialAssessmentDate();
        CsvCell initialAssessmentTime = parser.getInitialAssessmentTime();
        Date initialAssessmentDateTime = CsvCell.getDateTimeFromTwoCells(initialAssessmentDate, initialAssessmentTime);
        if (initialAssessmentDateTime != null) {
            stagingEmergencyCds.setInitialAssessmentDate(initialAssessmentDateTime);
        }
        stagingEmergencyCds.setChiefComplaint(parser.getChiefComplaint().getString());

        CsvCell seenForTreatmentDate = parser.getDateSeenforTreatment();
        CsvCell seenForTreatmentTime = parser.getTimeSeenforTreatment();
        Date seenForTreatmentDateTime = CsvCell.getDateTimeFromTwoCells(seenForTreatmentDate, seenForTreatmentTime);
        if (seenForTreatmentDateTime != null) {
            stagingEmergencyCds.setSeenForTreatmentDate(seenForTreatmentDateTime);
        }
        CsvCell decidedToAdmitDate = parser.getDecidedtoAdmitDate();
        CsvCell decidedToAdmitTime = parser.getDecidedtoAdmitTime();
        Date decidedToAdmitDateTime = CsvCell.getDateTimeFromTwoCells(decidedToAdmitDate, decidedToAdmitTime);
        if (decidedToAdmitDateTime != null) {
            stagingEmergencyCds.setDecidedToAdmitDate(decidedToAdmitDateTime);
        }
        stagingEmergencyCds.setTreatmentFunctionCode(parser.getActivityTreatmentFunctionCode().getString());
        stagingEmergencyCds.setDischargeStatus(parser.getDischargeStatus().getString());

        CsvCell conclusionDate = parser.getConclusionDate();
        CsvCell conclusionTime = parser.getConclusionTime();
        Date conclusionDateTime = CsvCell.getDateTimeFromTwoCells(conclusionDate, conclusionTime);
        if (conclusionDateTime != null) {
            stagingEmergencyCds.setConclusionDate(conclusionDateTime);
        }
        CsvCell departureDate = parser.getDepartureDate();
        CsvCell departureTime = parser.getDepartureTime();
        Date departureDateTime = CsvCell.getDateTimeFromTwoCells(departureDate, departureTime);
        if (departureDateTime != null) {
            stagingEmergencyCds.setDepartureDate(departureDateTime);
        }
        stagingEmergencyCds.setDischargeDestination(parser.getDischargeDestination().getString());
        stagingEmergencyCds.setDischargeDestinationSiteId(parser.getDischargeDestinationSiteId().getString());

        // process all Mental Health Classification data into a delimetered string format eg:
        // start datetime~end datetime~code|start datetime~end datetime~code
        List<String> mhClassificationsList = new ArrayList<>();
        int dataNumber = 1;
        while (dataNumber < 11) {

            //get next data cell (range could be from 1-10)
            String dataCode = parser.getMHClassificationCode(dataNumber).getString();
            //if no more data break out of loop
            if (Strings.isNullOrEmpty(dataCode)) {
                break;
            }
            String dataStartDate = parser.getMHClassificationStartDate(dataNumber).getString();
            String dataStartTime = parser.getMHClassificationStartTime(dataNumber).getString();
            String dataStartDateTime = dataStartDate.concat(" ").concat(dataStartTime).trim();
            String dataEndDate = parser.getMHClassificationEndDate(dataNumber).getString();
            String dataEndTime = parser.getMHClassificationEndTime(dataNumber).getString();
            String dataEndDateTime = dataEndDate.concat(" ").concat(dataEndTime).trim();

            mhClassificationsList.add(dataStartDateTime.concat("~").concat(dataEndDateTime).concat("~").concat(dataCode));

            dataNumber++;
        }
        //finally set the delimetered MH data string, sorted (to prevent duplicates) and pipe delimetered
        Collections.sort(mhClassificationsList);
        String mhClassifications = String.join("|", mhClassificationsList);
        stagingEmergencyCds.setMhClassifications(mhClassifications);

        // process all Diagnosis data into a delimetered string format eg:
        // code|code
        List<String> diagnosisList = new ArrayList<>();
        dataNumber = 1;
        while (dataNumber < 21) {

            //get next data cell (range could be from 1-20)
            String dataCode = parser.getDiagnosis(dataNumber).getString();
            //if no more data break out of loop
            if (Strings.isNullOrEmpty(dataCode)) {
                break;
            }
            diagnosisList.add(dataCode);

            dataNumber++;
        }
        //finally set the delimetered diagnosis data string
        Collections.sort(diagnosisList);
        String diagnosis = String.join("|",diagnosisList);
        stagingEmergencyCds.setDiagnosis(diagnosis);

        // process all Investigations data into a delimetered string format eg:
        // datetime~code
        List<String> invList = new ArrayList<>();
        dataNumber = 1;
        while (dataNumber < 21) {

            //get next data cell (range could be from 1-10)
            String dataCode = parser.getInvestigation(dataNumber).getString();
            //if no more data break out of loop
            if (Strings.isNullOrEmpty(dataCode)) {
                break;
            }
            String dataDate = parser.getInvestigationPerformedDate(dataNumber).getString();
            String dataTime = parser.getInvestigationPerformedTime(dataNumber).getString();
            String dataDateTime = dataDate.concat(" ").concat(dataTime);

            // Data is in the format  datetime~treatment code.
            // Often, the entries are duplicated, i.e. same code, date and time, so check before adding in new data again
            String data = dataDateTime.concat("~").concat(dataCode);
            if (!invList.contains(data)) {
                invList.add(data);
            }

            dataNumber++;
        }
        //finally set the delimetered investigations data string, sorted (to prevent duplicates) and pipe delimetered
        Collections.sort(invList);
        String inv = String.join("|", invList);
        stagingEmergencyCds.setInvestigations(inv);

        // process all Treatment data into a delimetered string format eg:
        // datetime~code
        List<String> treatmentList = new ArrayList<>();
        dataNumber = 1;
        while (dataNumber < 21) {

            //get next data cell (range could be from 1-10)
            String dataCode = parser.getTreatment(dataNumber).getString();
            //if no more data break out of loop
            if (Strings.isNullOrEmpty(dataCode)) {
                break;
            }
            String dataDate = parser.getTreatmentDate(dataNumber).getString();
            String dataTime = parser.getTreatmentTime(dataNumber).getString();
            String dataDateTime = dataDate.concat(" ").concat(dataTime);

            // Data is in the format  datetime~treatment code.
            // Often, the entries are duplicated, i.e. same code, date and time, so check before adding in new data again
            String data = dataDateTime.concat("~").concat(dataCode);
            if (!treatmentList.contains(data)) {
                treatmentList.add(dataDateTime.concat("~").concat(dataCode));
            }

            dataNumber++;
        }
        //finally set the date time delimetered treatment data string, sorted (to prevent duplicates) and pipe delimetered
        Collections.sort(treatmentList);
        String treatments = String.join("|", treatmentList);
        stagingEmergencyCds.setTreatments(treatments);

        // process all Referal data into a delimetered string format eg:
        // request date~assessment date~code
        List<String> referralList = new ArrayList<>();
        dataNumber = 1;
        while (dataNumber < 11) {

            //get next data cell (range could be from 1-10)
            String dataCode = parser.getReferralToService(dataNumber).getString();
            //if no more data break out of loop
            if (Strings.isNullOrEmpty(dataCode)) {
                break;
            }
            String requestDate = parser.getReferralRequestDate(dataNumber).getString();
            String requestTime = parser.getReferralRequestTime(dataNumber).getString();
            String requestDateTime = requestDate.concat(" ").concat(requestTime);
            String assessmentDate = parser.getReferralAssessmentDate(dataNumber).getString();
            String assessmentTime = parser.getReferralAssessmentTime(dataNumber).getString();
            String assessmentDateTime = assessmentDate.concat(" ").concat(assessmentTime);

            referralList.add(requestDateTime.concat("~").concat(assessmentDateTime).concat("~").concat(dataCode));

            dataNumber++;
        }
        //finally set the delimetered referrals data string, sorted (to prevent duplicates) and pipe delimetered
        Collections.sort(referralList);
        String referrals = String.join("|", referralList);
        stagingEmergencyCds.setReferredToServices(referrals);

        // process all Safe Guarding data into a delimetered string format eg:
        // code|code
        List<String> safeGuardlingList = new ArrayList<>();
        dataNumber = 1;
        while (dataNumber < 11) {

            //get next data cell (range could be from 1-20)
            String dataCode = parser.getSafeguardingConcern(dataNumber).getString();
            //if no more data break out of loop
            if (Strings.isNullOrEmpty(dataCode)) {
                break;
            }
            safeGuardlingList.add(dataCode);

            dataNumber++;
        }
        //finally set the delimetered safe guarding data string, sorted (to prevent duplicates) and pipe delimetered
        Collections.sort(safeGuardlingList);
        String safeGuardingConcerns = String.join("|", safeGuardlingList);
        stagingEmergencyCds.setSafeguardingConcerns(safeGuardingConcerns);

        //finally, add the Cds batch for saving
        emergencyCdsBatch.add(stagingEmergencyCds);
        saveEmergencyCdsBatch(emergencyCdsBatch, false, csvHelper);
    }

    private static void processOutpatients(CdsRecordOutpatientI parser, BartsCsvHelper csvHelper,
                                          List<StagingOutpatientCds> outpatientCdsBatch) throws Exception {

        StagingOutpatientCds stagingOutpatientCds = new StagingOutpatientCds();

        CsvCell cdsUniqueIdCell = parser.getCdsUniqueId();
        stagingOutpatientCds.setCdsUniqueIdentifier(cdsUniqueIdCell.getString());

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(cdsUniqueIdCell.getPublishedFileId(), cdsUniqueIdCell.getRecordNumber());
        stagingOutpatientCds.setAudit(audit);

        stagingOutpatientCds.setExchangeId(csvHelper.getExchangeId().toString());
        stagingOutpatientCds.setDtReceived(csvHelper.getDataDate());

        //Cds activity date is common to all Sus files so if it is missing, log and exit record
        CsvCell cdsActivityDateCell = parser.getCdsActivityDate();
        if (!cdsActivityDateCell.isEmpty()) {
            stagingOutpatientCds.setCdsActivityDate(parser.getCdsActivityDate().getDate());
        } else {
            TransformWarnings.log(LOG, csvHelper, "Missing Outpatient cdsActivityDate CDS id {}", parser.getCdsUniqueId());
            return;
        }

        stagingOutpatientCds.setCdsUpdateType(parser.getCdsUpdateType().getInt());

        CsvCell withheldCell = parser.getWithheldFlag();
        boolean isWithheld = withheldCell.getBoolean();
        stagingOutpatientCds.setWithheld(new Boolean(isWithheld));

        if (!isWithheld) { //LocalPatientId and NHS number should be empty if withheld. Get persondId from tail file
            String localPatientId = parser.getLocalPatientId().getString();
            stagingOutpatientCds.setMrn(localPatientId);
            stagingOutpatientCds.setNhsNumber(parser.getNhsNumber().getString());
            String personId = csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, localPatientId);
            if (Strings.isNullOrEmpty(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Failed to find personid for CDS id {}", parser.getCdsUniqueId());
                return;
            }
            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Skipping CDS record {} as not part of filtered subset", parser.getCdsUniqueId());
                return;
            }
            stagingOutpatientCds.setLookupPersonId(Integer.valueOf(personId));
        }
        stagingOutpatientCds.setDateOfBirth(parser.getPersonBirthDate().getDate());
        String consultantStr = parser.getConsultantCode().getString();
        stagingOutpatientCds.setConsultantCode(consultantStr);

        String personnelIdStr = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_CONSULTANT_TO_ID, consultantStr);
        if (!Strings.isNullOrEmpty(personnelIdStr)) {
            stagingOutpatientCds.setLookupConsultantPersonnelId(Integer.valueOf(personnelIdStr));
        }
        stagingOutpatientCds.setPatientPathwayIdentifier(parser.getPatientPathwayIdentifier().getString());

        stagingOutpatientCds.setApptAttendanceIdentifier(parser.getAttendanceIdentifier().getString());
        stagingOutpatientCds.setApptAttendedCode(parser.getAppointmentAttendedCode().getString());
        stagingOutpatientCds.setApptOutcomeCode(parser.getAppointmentOutcomeCode().getString());
        stagingOutpatientCds.setApptSiteCode(parser.getAppointmentSiteCode().getString());

        CsvCell apptStartDate = parser.getAppointmentDate();
        CsvCell apptStartTime = parser.getAppointmentTime();
        Date apptStartDateTime = CsvCell.getDateTimeFromTwoCells(apptStartDate, apptStartTime);
        if (apptStartDateTime != null) {
            stagingOutpatientCds.setApptDate(apptStartDateTime);
        }

        stagingOutpatientCds.setPrimaryDiagnosisICD(parser.getPrimaryDiagnosisICD().getString());
        stagingOutpatientCds.setSecondaryDiagnosisICD(parser.getSecondaryDiagnosisICD().getString());
        stagingOutpatientCds.setOtherDiagnosisICD(parser.getAdditionalSecondaryDiagnosisICD().getString());

        stagingOutpatientCds.setPrimaryProcedureOPCS(parser.getPrimaryProcedureOPCS().getString());
        CsvCell procDateCell = parser.getPrimaryProcedureDate();
        if (!procDateCell.isEmpty()) {
            stagingOutpatientCds.setPrimaryProcedureDate(procDateCell.getDate());
        }
        stagingOutpatientCds.setSecondaryProcedureOPCS(parser.getSecondaryProcedureOPCS().getString());
        CsvCell proc2DateCell = parser.getSecondaryProcedureDate();
        if (!proc2DateCell.isEmpty()) {
            stagingOutpatientCds.setSecondaryProcedureDate(proc2DateCell.getDate());
        }
        stagingOutpatientCds.setOtherProceduresOPCS(parser.getAdditionalSecondaryProceduresOPCS().getString());

        outpatientCdsBatch.add(stagingOutpatientCds);
        saveOutpatientCdsBatch(outpatientCdsBatch, false, csvHelper);
    }

    private static void processInpatients(CdsRecordInpatientI parser, BartsCsvHelper csvHelper,
                                          List<StagingInpatientCds> inpatientCdsBatch) throws Exception {

        StagingInpatientCds stagingInpatientCds = new StagingInpatientCds();

        CsvCell cdsUniqueIdCell = parser.getCdsUniqueId();
        stagingInpatientCds.setCdsUniqueIdentifier(cdsUniqueIdCell.getString());

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(cdsUniqueIdCell.getPublishedFileId(), cdsUniqueIdCell.getRecordNumber());
        stagingInpatientCds.setAudit(audit);

        stagingInpatientCds.setExchangeId(csvHelper.getExchangeId().toString());
        stagingInpatientCds.setDtReceived(csvHelper.getDataDate());

        //Cds activity date is common to all Sus files so if it is missing, log and exit record
        CsvCell cdsActivityDateCell = parser.getCdsActivityDate();
        if (!cdsActivityDateCell.isEmpty()) {
            stagingInpatientCds.setCdsActivityDate(parser.getCdsActivityDate().getDate());
        } else {
            TransformWarnings.log(LOG, csvHelper, "Missing Inpatient cdsActivityDate CDS id {}", parser.getCdsUniqueId());
            return;
        }

        stagingInpatientCds.setCdsUpdateType(parser.getCdsUpdateType().getInt());

        CsvCell withheldCell = parser.getWithheldFlag();
        boolean isWithheld = withheldCell.getBoolean();
        stagingInpatientCds.setWithheld(new Boolean(isWithheld));

        if (!isWithheld) { //LocalPatientId and NHS number should be empty if withheld. Get persondId from tail file
            String localPatientId = parser.getLocalPatientId().getString();
            stagingInpatientCds.setMrn(localPatientId);
            stagingInpatientCds.setNhsNumber(parser.getNhsNumber().getString());
            String personId = csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, localPatientId);
            if (Strings.isNullOrEmpty(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Failed to find personid for CDS id {}", parser.getCdsUniqueId());
                return;
            }
            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Skipping CDS record {} as not part of filtered subset", parser.getCdsUniqueId());
                return;
            }
            stagingInpatientCds.setLookupPersonId(Integer.valueOf(personId));
        }
        stagingInpatientCds.setDateOfBirth(parser.getPersonBirthDate().getDate());
        String consultantStr = parser.getConsultantCode().getString();
        stagingInpatientCds.setConsultantCode(consultantStr);

        String personnelIdStr = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_CONSULTANT_TO_ID, consultantStr);
        if (!Strings.isNullOrEmpty(personnelIdStr)) {
            stagingInpatientCds.setLookupConsultantPersonnelId(Integer.valueOf(personnelIdStr));
        }
        stagingInpatientCds.setPatientPathwayIdentifier(parser.getPatientPathwayIdentifier().getString());

        stagingInpatientCds.setSpellNumber(parser.getHospitalSpellNumber().getString());
        stagingInpatientCds.setAdmissionMethodCode(parser.getAdmissionMethodCode().getString());
        stagingInpatientCds.setAdmissionSourceCode(parser.getAdmissionSourceCode().getString());
        stagingInpatientCds.setPatientClassification(parser.getPatientClassification().getString());

        CsvCell spellStartDate = parser.getHospitalSpellStartDate();
        CsvCell spellStartTime = parser.getHospitalSpellStartTime();
        Date spellStartDateTime = CsvCell.getDateTimeFromTwoCells(spellStartDate, spellStartTime);
        if (spellStartDateTime != null) {
            stagingInpatientCds.setSpellStartDate(spellStartDateTime);
        }
        stagingInpatientCds.setEpisodeNumber(parser.getEpisodeNumber().getString());
        stagingInpatientCds.setEpisodeStartSiteCode(parser.getEpisodeStartSiteCode().getString());
        stagingInpatientCds.setEpisodeStartWardCode(parser.getEpisodeStartWardCode().getString());

        CsvCell episodeStartDate = parser.getEpisodeStartDate();
        CsvCell episodeStartTime = parser.getEpisodeStartTime();
        Date episodeStartDateTime = CsvCell.getDateTimeFromTwoCells(episodeStartDate, episodeStartTime);
        if (episodeStartDateTime != null) {
            stagingInpatientCds.setEpisodeStartDate(episodeStartDateTime);
        }
        stagingInpatientCds.setEpisodeEndSiteCode(parser.getEpisodeEndSiteCode().getString());
        stagingInpatientCds.setEpisodeEndWardCode(parser.getEpisodeEndWardCode().getString());

        CsvCell episodeEndDate = parser.getEpisodeEndDate();
        CsvCell episodeEndTime = parser.getEpisodeEndTime();
        Date episodeEndDateTime = CsvCell.getDateTimeFromTwoCells(episodeEndDate, episodeEndTime);
        if (episodeEndDateTime != null) {
            stagingInpatientCds.setEpisodeEndDate(episodeEndDateTime);
        }
        CsvCell dischargeDate = parser.getDischargeDate();
        CsvCell dischargeTime = parser.getDischargeTime();
        Date dischargeDateTime = CsvCell.getDateTimeFromTwoCells(dischargeDate, dischargeTime);
        if (dischargeDateTime != null) {
            stagingInpatientCds.setDischargeDate(dischargeDateTime);
        }
        stagingInpatientCds.setDischargeDestinationCode(parser.getDischargeDestinationCode().getString());
        stagingInpatientCds.setDischargeMethod(parser.getDischargeMethod().getString());

        stagingInpatientCds.setPrimaryDiagnosisICD(parser.getPrimaryDiagnosisICD().getString());
        stagingInpatientCds.setSecondaryDiagnosisICD(parser.getSecondaryDiagnosisICD().getString());
        stagingInpatientCds.setOtherDiagnosisICD(parser.getAdditionalSecondaryDiagnosisICD().getString());

        stagingInpatientCds.setPrimaryProcedureOPCS(parser.getPrimaryProcedureOPCS().getString());
        CsvCell procDateCell = parser.getPrimaryProcedureDate();
        if (!procDateCell.isEmpty()) {
            stagingInpatientCds.setPrimaryProcedureDate(procDateCell.getDate());
        }
        stagingInpatientCds.setSecondaryProcedureOPCS(parser.getSecondaryProcedureOPCS().getString());
        CsvCell proc2DateCell = parser.getSecondaryProcedureDate();
        if (!proc2DateCell.isEmpty()) {
            stagingInpatientCds.setSecondaryProcedureDate(proc2DateCell.getDate());
        }
        stagingInpatientCds.setOtherProceduresOPCS(parser.getAdditionalSecondaryProceduresOPCS().getString());

        inpatientCdsBatch.add(stagingInpatientCds);
        saveInpatientCdsBatch(inpatientCdsBatch, false, csvHelper);
    }

    private static void processHomeDelBirths(CdsRecordInpatientI parser, BartsCsvHelper csvHelper,
                                          List<StagingHomeDelBirthCds> homeDelBirthCdsBatch) throws Exception {

        StagingHomeDelBirthCds stagingHomeDelBirthCds = new StagingHomeDelBirthCds();

        CsvCell cdsUniqueIdCell = parser.getCdsUniqueId();
        stagingHomeDelBirthCds.setCdsUniqueIdentifier(cdsUniqueIdCell.getString());

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(cdsUniqueIdCell.getPublishedFileId(), cdsUniqueIdCell.getRecordNumber());
        stagingHomeDelBirthCds.setAudit(audit);

        stagingHomeDelBirthCds.setExchangeId(csvHelper.getExchangeId().toString());
        stagingHomeDelBirthCds.setDtReceived(csvHelper.getDataDate());

        //Cds activity date is common to all Sus files so if it is missing, log and exit record
        CsvCell cdsActivityDateCell = parser.getCdsActivityDate();
        if (!cdsActivityDateCell.isEmpty()) {
            stagingHomeDelBirthCds.setCdsActivityDate(parser.getCdsActivityDate().getDate());
        } else {
            TransformWarnings.log(LOG, csvHelper, "Missing HomeDelBirth cdsActivityDate CDS id {}", parser.getCdsUniqueId());
            return;
        }

        stagingHomeDelBirthCds.setCdsUpdateType(parser.getCdsUpdateType().getInt());

        CsvCell withheldCell = parser.getWithheldFlag();
        boolean isWithheld = withheldCell.getBoolean();
        stagingHomeDelBirthCds.setWithheld(new Boolean(isWithheld));

        if (!isWithheld) { //LocalPatientId and NHS number should be empty if withheld. Get persondId from tail file
            String localPatientId = parser.getLocalPatientId().getString();
            stagingHomeDelBirthCds.setMrn(localPatientId);
            stagingHomeDelBirthCds.setNhsNumber(parser.getNhsNumber().getString());
            String personId = csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, localPatientId);
            if (Strings.isNullOrEmpty(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Failed to find personid for CDS id {}", parser.getCdsUniqueId());
                return;
            }
            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Skipping CDS record {} as not part of filtered subset", parser.getCdsUniqueId());
                return;
            }
            stagingHomeDelBirthCds.setLookupPersonId(Integer.valueOf(personId));
        }
        stagingHomeDelBirthCds.setDateOfBirth(parser.getPersonBirthDate().getDate());

        stagingHomeDelBirthCds.setBirthWeight(parser.getBirthWeight().getString());
        stagingHomeDelBirthCds.setLiveOrStillBirthIndicator(parser.getLiveOrStillBirthIndicator().getString());
        stagingHomeDelBirthCds.setTotalPreviousPregnancies(parser.getTotalPreviousPregnancies().getString());

        stagingHomeDelBirthCds.setNumberOfBabies(parser.getNumberOfBabies().getInt());
        stagingHomeDelBirthCds.setFirstAntenatalAssessmentDate(parser.getFirstAntenatalAssessmentDate().getDate());
        stagingHomeDelBirthCds.setAntenatalCarePractitioner(parser.getAntenatalCarePractitioner().getString());
        stagingHomeDelBirthCds.setAntenatalCarePractice(parser.getAntenatalCarePractice().getString());
        stagingHomeDelBirthCds.setDeliveryPlaceIntended(parser.getDeliveryPlaceTypeIntended().getString());
        stagingHomeDelBirthCds.setDeliveryPlaceChangeReasonCode(parser.getDeliveryPlaceChangeReasonCode().getString());
        stagingHomeDelBirthCds.setGestationLengthLabourOnset(parser.getGestationLengthLabourOnset().getString());
        stagingHomeDelBirthCds.setDeliveryDate(parser.getDeliveryDate().getDate());
        stagingHomeDelBirthCds.setDeliveryPlaceActual(parser.getDeliveryPlaceTypeActual().getString());
        stagingHomeDelBirthCds.setDeliveryMethod(parser.getDeliveryMethod().getString());
        stagingHomeDelBirthCds.setMotherNhsNumber(parser.getMotherNHSNumber().getString());

        homeDelBirthCdsBatch.add(stagingHomeDelBirthCds);
        saveHomeDelBirthCdsBatch(homeDelBirthCdsBatch, false, csvHelper);
    }
}
