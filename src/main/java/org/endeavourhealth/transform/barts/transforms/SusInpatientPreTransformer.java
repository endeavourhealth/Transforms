package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingCdsDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCds;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsSusHelper;
import org.endeavourhealth.transform.barts.schema.SusInpatient;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class SusInpatientPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SusInpatientPreTransformer.class);

    private static StagingCdsDalI repository = DalProvider.factoryStagingCdsDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {
        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((org.endeavourhealth.transform.barts.schema.SusInpatient) parser, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

    }

    private static void processRecord(SusInpatient parser, BartsCsvHelper csvHelper) throws Exception {

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
        stagingCds.setExchangeId(parser.getExchangeId().toString());
        stagingCds.setDtReceived(new Date());
        stagingCds.setCdsActivityDate(parser.getCdsActivityDate().getDate());
        stagingCds.setSusRecordType(BartsCsvHelper.SUS_RECORD_TYPE_INPATIENT);
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
        }

        //primary procedure
        parsePrimaryProcedure(parser, stagingCds, csvHelper);

        //Secondary
        parseSecondaryProcedure(parser, stagingCds, csvHelper);

        //Rest
        parseRemainingProcedures(parser, stagingCds, csvHelper);

    }

    private static void parsePrimaryProcedure(SusInpatient parser, StagingCds commonContent, BartsCsvHelper csvHelper) throws Exception {
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
        if (dateCell.isEmpty()) {
            TransformWarnings.log(LOG, csvHelper, "Missing primary procedure date {} for inpatient CDS record", dateCell);
            return;
        }
        cdsPrimary.setProcedureDate(parser.getPrimaryProcedureDate().getDate());

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveDataCallable(parser.getCurrentState(), cdsPrimary, serviceId));

        //for secondary etc. we set the primary opcs code on a separate column so set on the common object
        commonContent.setPrimaryProcedureOpcsCode(opcsCode);

    }

    private static void parseSecondaryProcedure(SusInpatient parser, StagingCds commonContent, BartsCsvHelper csvHelper) throws Exception {
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
        if (dateCell.isEmpty()) {
            TransformWarnings.log(LOG, csvHelper, "Missing secondary procedure date for {} inpatient CDS record", dateCell);
            dateCell = parser.getPrimaryProcedureDate();
            if (dateCell.isEmpty()) {
                TransformWarnings.log(LOG, csvHelper, "Skipping secondary procedure because primary date {} is empty", dateCell);
                return;
            }
        }
        cdsSecondary.setProcedureDate(dateCell.getDate());

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SaveDataCallable(parser.getCurrentState(), cdsSecondary, serviceId));

    }

    private static void parseRemainingProcedures(SusInpatient parser, StagingCds commonContent, BartsCsvHelper csvHelper) throws Exception {
        CsvCell otherProcedureOPCS = parser.getAdditionalecondaryProceduresOPCS();
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
                TransformWarnings.log(LOG, csvHelper, "Missing " + seq + " procedure date {} for inpatient CDS record", otherProcedureOPCS);
                CsvCell dateCell = parser.getPrimaryProcedureDate();
                if (dateCell.isEmpty()) {
                    TransformWarnings.log(LOG, csvHelper, "Skipping secondary procedure because primary date {} is empty", dateCell);
                    continue;
                }

                cdsRemainder.setProcedureDate(dateCell.getDate());
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
