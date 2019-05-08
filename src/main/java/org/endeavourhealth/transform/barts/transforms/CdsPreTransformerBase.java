package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingCdsDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCds;
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

    protected static void processProcedures(CdsRecordI parser, BartsCsvHelper csvHelper, String susRecordType) throws Exception {

        //if no procedures, then nothing to save
        CsvCell primaryProcedureCell = parser.getPrimaryProcedureOPCS();
        if (primaryProcedureCell.isEmpty()) {
            return;
        }
        if (parser.getWithheldFlag().getIntAsBoolean()) {
            return;
        }
        StagingCds stagingCds = new StagingCds();
        stagingCds.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
        stagingCds.setExchangeId(csvHelper.getExchangeId().toString());
        stagingCds.setDtReceived(new Date());
        stagingCds.setCdsActivityDate(parser.getCdsActivityDate().getDate());
        stagingCds.setSusRecordType(susRecordType);
        stagingCds.setCdsUpdateType(parser.getCdsUpdateType().getInt());
        stagingCds.setMrn(parser.getLocalPatientId().getString());
        stagingCds.setNhsNumber(parser.getNhsNumber().getString());
        stagingCds.setDateOfBirth(parser.getPersonBirthDate().getDate());
        String consultantStr = parser.getConsultantCode().getString();
        stagingCds.setConsultantCode(consultantStr);

        String personnelIdStr = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_CONSULTANT_TO_ID, consultantStr);
        if (!Strings.isNullOrEmpty(personnelIdStr)) {
            stagingCds.setLookupConsultantPersonnelId(Integer.valueOf(personnelIdStr));
        }

        String localPatientId = parser.getLocalPatientId().getString();
        String personId = csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, localPatientId);
        if (!Strings.isNullOrEmpty(personId)) {
            stagingCds.setLookupPersonId(Integer.valueOf(personId));

            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                return;
            }
        } else {
            TransformWarnings.log(LOG, csvHelper, "Failed to find personid for procedure id {}", parser.getCdsUniqueId());
            return;
        }

        //primary procedure
        parsePrimaryProcedure(parser, stagingCds, csvHelper);

        //Secondary
        parseSecondaryProcedure(parser, stagingCds, csvHelper);

        //Rest
        parseRemainingProcedures(parser, stagingCds, csvHelper);

    }


    private static void parsePrimaryProcedure(CdsRecordI parser, StagingCds commonContent, BartsCsvHelper csvHelper) throws Exception {
        StagingCds cdsPrimary = commonContent.clone();

        String opcsCode = parser.getPrimaryProcedureOPCS().getString();
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
        csvHelper.submitToThreadPool(new SaveDataCallable(parser.getCurrentState(), cdsPrimary, serviceId));

        //for secondary etc. we set the primary opcs code on a separate column so set on the common object
        commonContent.setPrimaryProcedureOpcsCode(opcsCode);

    }

    private static void parseSecondaryProcedure(CdsRecordI parser, StagingCds commonContent, BartsCsvHelper csvHelper) throws Exception {
        CsvCell secondaryProcedureCell = parser.getSecondaryProcedureOPCS();
        if (secondaryProcedureCell.isEmpty()) {
            //if no secondary procedure, then we're finished
            return;
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
        csvHelper.submitToThreadPool(new SaveDataCallable(parser.getCurrentState(), cdsSecondary, serviceId));

    }

    private static void parseRemainingProcedures(CdsRecordI parser, StagingCds commonContent, BartsCsvHelper csvHelper) throws Exception {
        CsvCell otherProcedureOPCS = parser.getAdditionalSecondaryProceduresOPCS();
        if (otherProcedureOPCS.isEmpty()) {
            return;
        }

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
            csvHelper.submitToThreadPool(new SaveDataCallable(parser.getCurrentState(), cdsRemainder, serviceId));
            seq++;
        }
    }

    private static class SaveDataCallable extends AbstractCsvCallable {

        private StagingCds obj = null;
        private UUID serviceId;

        public SaveDataCallable(CsvCurrentState parserState,
                                StagingCds obj,
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