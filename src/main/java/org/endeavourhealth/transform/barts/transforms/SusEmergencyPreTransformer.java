package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingCdsDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCds;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsSusHelper;
import org.endeavourhealth.transform.barts.schema.SusEmergency;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class SusEmergencyPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SusEmergencyPreTransformer.class);

    private static StagingCdsDalI repository = DalProvider.factoryStagingCdsDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {
        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((SusEmergency) parser, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

    }

    private static void processRecord(SusEmergency parser, BartsCsvHelper csvHelper) throws Exception {

        //if no procedures, then nothing to save
        CsvCell primaryProcedureCell = parser.getPrimaryProcedureOPCS();
        if (primaryProcedureCell.isEmpty()) {
            return;
        }

        StagingCds stagingCds = new StagingCds();
        stagingCds.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
        stagingCds.setExchangeId(parser.getExchangeId().toString());
        stagingCds.setDtReceived(new Date());
        stagingCds.setCdsActivityDate(parser.getCdsActivityDate().getDate());
        stagingCds.setSusRecordType(BartsCsvHelper.SUS_RECORD_TYPE_EMERGENCY);
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
        parseRemaningProcedures(parser, stagingCds, csvHelper);

    }

    private static void parsePrimaryProcedure(SusEmergency parser, StagingCds commonContent, BartsCsvHelper csvHelper) throws Exception {
        StagingCds cdsPrimary = commonContent.clone();

        String opcsCode = parser.getPrimaryProcedureOPCS().getString();
        opcsCode = TerminologyService.standardiseOpcs4Code(opcsCode);
        cdsPrimary.setProcedureOpcsCode(opcsCode);

        String term = TerminologyService.lookupOpcs4ProcedureName(opcsCode);
        if (!Strings.isNullOrEmpty(term)) {
            throw new Exception("Failed to find term for OPCS-4 code " + opcsCode);
        }
        cdsPrimary.setLookupProcedureOpcsTerm(term);

        cdsPrimary.setProcedureSeqNbr(1);

        if (parser.getPrimaryProcedureDate().isEmpty()) {
            //TODO - how is this logging going to be picked up? Shouldn't this be logged using TransformWarning, so it's in the DB?
            LOG.warn("Missing primary date for " + parser.getCdsUniqueId());
            return;
        }
        cdsPrimary.setProcedureDate(parser.getPrimaryProcedureDate().getDate());

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SusEmergencyPreTransformer.saveDataCallable(parser.getCurrentState(), cdsPrimary, serviceId));

        //for secondary etc. we set the primary opcs code on a separate column so set on the common object
        commonContent.setPrimaryProcedureOpcsCode(opcsCode);

    }

    private static void parseSecondaryProcedure(SusEmergency parser, StagingCds commonContent, BartsCsvHelper csvHelper) throws Exception {
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
        if (!Strings.isNullOrEmpty(term)) {
            throw new Exception("Failed to find term for OPCS-4 code " + opcsCode);
        }
        cdsSecondary.setLookupProcedureOpcsTerm(term);
        cdsSecondary.setProcedureSeqNbr(2);

        if (parser.getSecondaryProcedureDate().isEmpty()) {
            //TODO - how is this logging going to be picked up? Shouldn't it be logged using TransformWarning?
            LOG.warn("Missing secondary date for " + parser.getCdsUniqueId());
            cdsSecondary.setProcedureDate(parser.getPrimaryProcedureDate().getDate());
        } else {
            cdsSecondary.setProcedureDate(parser.getSecondaryProcedureDate().getDate());
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SusEmergencyPreTransformer.saveDataCallable(parser.getCurrentState(), cdsSecondary, serviceId));

    }

    private static void parseRemaningProcedures(SusEmergency parser, StagingCds commonContent, BartsCsvHelper csvHelper) throws Exception {
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

            if (word.length() > 4) {
                String dateStr = word.substring(4);
                if (Strings.isNullOrEmpty(dateStr)) {
                    //TODO - why are we breaking out here? If there's no date for the primary or secondary procedures we log stuff out - in this case we don't. Why not?
                    break;
                }
                Date date = parser.getDateFormat().parse(dateStr);
                cdsRemainder.setProcedureDate(date);
            }

            String term = TerminologyService.lookupOpcs4ProcedureName(opcsCode);
            if (!Strings.isNullOrEmpty(term)) {
                throw new Exception("Failed to find term for OPCS-4 code " + opcsCode);
            }
            cdsRemainder.setLookupProcedureOpcsTerm(term);

            cdsRemainder.setProcedureSeqNbr(seq);

            UUID serviceId = csvHelper.getServiceId();
            csvHelper.submitToThreadPool(new SusEmergencyPreTransformer.saveDataCallable(parser.getCurrentState(), cdsRemainder, serviceId));
            seq++;
        }
    }

    private static class saveDataCallable extends AbstractCsvCallable {

        private StagingCds obj = null;
        private UUID serviceId;

        public saveDataCallable(CsvCurrentState parserState,
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
