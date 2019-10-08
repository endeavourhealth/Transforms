package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingCdsDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingConditionCds;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingConditionCdsCount;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedureCds;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedureCdsCount;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsSusHelper;
import org.endeavourhealth.transform.barts.schema.CdsRecordI;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public abstract class CdsPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(CdsPreTransformerBase.class);

    private static StagingCdsDalI repository = DalProvider.factoryStagingCdsDalI();
    private final static String BARTS_UNKNOWN_OPCS_CODE_Y926 = "Barts unable to provide term for Y92.6";
    protected static void processRecords(CdsRecordI parser, BartsCsvHelper csvHelper, String susRecordType,
                                         List<StagingProcedureCds> procedureBatch,
                                         List<StagingProcedureCdsCount> procedureCountBatch,
                                         List<StagingConditionCds> conditionBatch,
                                         List<StagingConditionCdsCount> conditionCountBatch) throws Exception {

        if (TransformConfig.instance().isLive()) {
            processDiagnoses(parser, csvHelper, susRecordType, conditionBatch, conditionCountBatch);
            processProcedures(parser, csvHelper, susRecordType, procedureBatch, procedureCountBatch);

        } else {
            //on Cerner Transform server, just run diagnoses for now
            processDiagnoses(parser, csvHelper, susRecordType, conditionBatch, conditionCountBatch);
            //processProcedures(parser, csvHelper, susRecordType, procedureBatch, procedureCountBatch);
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
                if (opcsCode.equals("Y92.6")) {
                    term = BARTS_UNKNOWN_OPCS_CODE_Y926;
                    TransformWarnings.log(LOG, csvHelper,BARTS_UNKNOWN_OPCS_CODE_Y926);
                } else {
                    throw new Exception("Failed to find term for OPCS-4 code " + opcsCode);
                }
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
            throw new Exception("Failed to find term for ICD-10 code " + icdCode);
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
                throw new Exception("Failed to find term for ICD-10 code " + icdCode);
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
}
