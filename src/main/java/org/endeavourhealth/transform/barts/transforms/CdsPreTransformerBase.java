package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingCdsDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCds;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCdsCount;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingConditionCds;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingConditionCdsCount;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsSusHelper;
import org.endeavourhealth.transform.barts.schema.CdsRecordI;
import org.endeavourhealth.transform.common.AbstractCsvCallable;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public abstract class CdsPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(CdsPreTransformerBase.class);

    private static StagingCdsDalI repository = DalProvider.factoryStagingCdsDalI();

    protected static void processRecords(CdsRecordI parser, BartsCsvHelper csvHelper, String susRecordType) throws Exception {
        processProcedures(parser,csvHelper,susRecordType);
        processDiagnoses(parser,csvHelper,susRecordType);
    }

    private static void processProcedures(CdsRecordI parser, BartsCsvHelper csvHelper, String susRecordType) throws Exception {

        StagingCds stagingCds = new StagingCds();
        stagingCds.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
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
        if (parsePrimaryProcedure(parser, stagingCds, csvHelper)) {
            procedureCount ++;
        }

        //Secondary
        if (parseSecondaryProcedure(parser, stagingCds, csvHelper)) {
            procedureCount ++;
        }

        //Rest
        procedureCount += parseRemainingProcedures(parser, stagingCds, csvHelper);

        //persist the count of procedures in this cds record
        StagingCdsCount stagingCdsCount = new StagingCdsCount();
        stagingCdsCount.setExchangeId(parser.getCdsUniqueId().getString());
        stagingCdsCount.setDtReceived(csvHelper.getDataDate());
        stagingCdsCount.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
        stagingCdsCount.setSusRecordType(susRecordType);
        stagingCdsCount.setProcedureCount(procedureCount);

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsCountCallable(parser.getCurrentState(), stagingCdsCount, serviceId));
    }

    private static boolean parsePrimaryProcedure(CdsRecordI parser, StagingCds commonContent, BartsCsvHelper csvHelper) throws Exception {

        //if no procedures, then nothing to save
        CsvCell primaryProcedureCell = parser.getPrimaryProcedureOPCS();
        if (primaryProcedureCell.isEmpty()) {
            return false;
        }

        StagingCds cdsPrimary = commonContent.clone();

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

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsCallable(parser.getCurrentState(), cdsPrimary, serviceId));

        //for secondary etc. we set the primary opcs code on a separate column so set on the common object
        commonContent.setPrimaryProcedureOpcsCode(opcsCode);

        return true;
    }

    private static boolean parseSecondaryProcedure(CdsRecordI parser, StagingCds commonContent, BartsCsvHelper csvHelper) throws Exception {
        CsvCell secondaryProcedureCell = parser.getSecondaryProcedureOPCS();
        if (secondaryProcedureCell.isEmpty()) {
            //if no secondary procedure, then we're finished
            return false;
        }

        StagingCds cdsSecondary = commonContent.clone();

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

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsCallable(parser.getCurrentState(), cdsSecondary, serviceId));

        return true;
    }

    private static int parseRemainingProcedures(CdsRecordI parser, StagingCds commonContent, BartsCsvHelper csvHelper) throws Exception {
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
            StagingCds cdsRemainder = commonContent.clone();

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
                throw new Exception("Failed to find term for OPCS-4 code " + opcsCode);
            }
            cdsRemainder.setLookupProcedureOpcsTerm(term);

            cdsRemainder.setProcedureSeqNbr(seq);

            UUID serviceId = csvHelper.getServiceId();
            csvHelper.submitToThreadPool(new SaveCdsCallable(parser.getCurrentState(), cdsRemainder, serviceId));
            seq++;
            remainingProcedureCount++;
        }

        return remainingProcedureCount;
    }

    private static class SaveCdsCallable extends AbstractCsvCallable {

        private StagingCds obj = null;
        private UUID serviceId;

        public SaveCdsCallable(CsvCurrentState parserState,
                                StagingCds obj,
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

    private static class SaveCdsCountCallable extends AbstractCsvCallable {

        private StagingCdsCount obj = null;
        private UUID serviceId;

        public SaveCdsCountCallable(CsvCurrentState parserState,
                                StagingCdsCount obj,
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

    private static void processDiagnoses(CdsRecordI parser, BartsCsvHelper csvHelper, String susRecordType) throws Exception {

        StagingConditionCds stagingConditionCds = new StagingConditionCds();
        stagingConditionCds.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
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
        if (parsePrimaryDiagnosis(parser, stagingConditionCds, csvHelper)) {
            conditionCount ++;
        }

        //Secondary
        if (parseSecondaryDiagnosis(parser, stagingConditionCds, csvHelper)) {
            conditionCount ++;
        }

        //Rest
        conditionCount += parseRemainingDiagnoses(parser, stagingConditionCds, csvHelper);

        //persist the count of conditions in this cds record
        StagingConditionCdsCount stagingConditionCdsCount = new StagingConditionCdsCount();
        stagingConditionCdsCount.setExchangeId(parser.getCdsUniqueId().getString());
        stagingConditionCdsCount.setDtReceived(csvHelper.getDataDate());
        stagingConditionCdsCount.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
        stagingConditionCdsCount.setSusRecordType(susRecordType);
        stagingConditionCdsCount.setConditionCount(conditionCount);

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsConditionCountCallable(parser.getCurrentState(), stagingConditionCdsCount, serviceId));
    }


    private static boolean parsePrimaryDiagnosis(CdsRecordI parser, StagingConditionCds commonContent, BartsCsvHelper csvHelper) throws Exception {

        //if no diagnoses, then nothing to save
        CsvCell primaryDiagnosisCell = parser.getPrimaryDiagnosisICD();
        if (primaryDiagnosisCell.isEmpty()) {
            return false;
        }

        StagingConditionCds cdsPrimary = commonContent.clone();

        String icdCode = primaryDiagnosisCell.getString();
        icdCode = TerminologyService.standardiseIcd10Code(icdCode);
        cdsPrimary.setDiagnosisIcdCode(icdCode);

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

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsConditionCallable(parser.getCurrentState(), cdsPrimary, serviceId));

        //for secondary etc. we set the primary opcs code on a separate column so set on the common object
        commonContent.setPrimaryDiagnosisIcdCodee(icdCode);

        return true;
    }

    private static boolean parseSecondaryDiagnosis(CdsRecordI parser, StagingConditionCds commonContent, BartsCsvHelper csvHelper) throws Exception {
        CsvCell secondaryDiagnosisCell = parser.getSecondaryDiagnosisICD();
        if (secondaryDiagnosisCell.isEmpty()) {
            //if no secondary diagnosis, then we're finished
            return false;
        }

        StagingConditionCds cdsSecondary = commonContent.clone();

        String icdCode = secondaryDiagnosisCell.getString();
        icdCode = TerminologyService.standardiseIcd10Code(icdCode);
        cdsSecondary.setDiagnosisIcdCode(icdCode);

        String term = TerminologyService.lookupIcd10CodeDescription(icdCode);
        if (Strings.isNullOrEmpty(term)) {
            throw new Exception("Failed to find term for ICD 10 code " + icdCode);
        }
        cdsSecondary.setLookupDiagnosisIcdTerm(term);
        cdsSecondary.setDiagnosisSeqNbr(2);

        CsvCell dateCell = parser.getCdsActivityDate();
        if (!dateCell.isEmpty()) {
            cdsSecondary.setCdsActivityDate(dateCell.getDate());
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveCdsConditionCallable(parser.getCurrentState(), cdsSecondary, serviceId));

        return true;
    }

    private static int parseRemainingDiagnoses(CdsRecordI parser, StagingConditionCds commonContent, BartsCsvHelper csvHelper) throws Exception {
        CsvCell otherDiagnosisICD = parser.getAdditionalSecondaryDiagnosisICD();
        if (otherDiagnosisICD.isEmpty()) {
            return 0;
        }

        int remainingConditionCount = 0;
        int seq = 3;

        for (String word : BartsSusHelper.splitEqually(otherDiagnosisICD.getString(), 40)) {
            if (Strings.isNullOrEmpty(word)) {
                break;
            }
            StagingConditionCds cdsRemainder = commonContent.clone();

            String icdCode = word.substring(0, 4);
            icdCode = icdCode.trim(); //because we sometimes get just three chars e.g. S41
            if (icdCode.isEmpty()) {
                break;
            }
            icdCode = TerminologyService.standardiseIcd10Code(icdCode);
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

            UUID serviceId = csvHelper.getServiceId();
            csvHelper.submitToThreadPool(new SaveCdsConditionCallable(parser.getCurrentState(), cdsRemainder, serviceId));
            seq++;
            remainingConditionCount++;
        }

        return remainingConditionCount;
    }

    private static class SaveCdsConditionCallable extends AbstractCsvCallable {

        private StagingConditionCds obj = null;
        private UUID serviceId;

        public SaveCdsConditionCallable(CsvCurrentState parserState,
                                        StagingConditionCds obj,
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

    private static class SaveCdsConditionCountCallable extends AbstractCsvCallable {

        private StagingConditionCdsCount obj = null;
        private UUID serviceId;

        public SaveCdsConditionCountCallable(CsvCurrentState parserState,
                                             StagingConditionCdsCount obj,
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
