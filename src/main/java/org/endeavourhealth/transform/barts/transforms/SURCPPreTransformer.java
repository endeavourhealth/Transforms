package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingSURCPDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingSURCP;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.SURCP;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class SURCPPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SURCPPreTransformer.class);

    private static StagingSURCPDalI repository = DalProvider.factoryStagingSURCPDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {
        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((SURCP) parser, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    private static void processRecord(SURCP parser, BartsCsvHelper csvHelper) throws Exception {

        StagingSURCP stagingSURCP = new StagingSURCP();
        stagingSURCP.setExchangeId(parser.getExchangeId().toString());
        stagingSURCP.setDtReceived(new Date());

        stagingSURCP.setSurgicalCaseProcedureId(parser.getSurgicalCaseProcedureId().getInt());
        stagingSURCP.setDtExtract(parser.getExtractDateTime().getDate());

        boolean activeInd = parser.getActiveIndicator().getIntAsBoolean();
        stagingSURCP.setActiveInd(activeInd);

        if (activeInd) {

            int caseId = parser.getSurgicalCaseId().getInt();
            stagingSURCP.setSurgicalCaseId(caseId);
            if (csvHelper.getPersonIdFromSurccId(caseId) == null) {
                return;
            }

            CsvCell procedureCodeCell = parser.getProcedureCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(procedureCodeCell)) {

                stagingSURCP.setProcedureCode(procedureCodeCell.getInt());

                //get lookup term from non 0 code
                CernerCodeValueRef codeValueRef = csvHelper.lookupCodeRef(CodeValueSet.PROCEDURE_ORDERS, procedureCodeCell);
                if (codeValueRef == null) {
                    throw new Exception("Failed to find CVREF record for code " + procedureCodeCell.getString());
                }
                String codeLookupTerm = codeValueRef.getCodeDispTxt();
                stagingSURCP.setLookupProcedureCodeTerm(codeLookupTerm);
            }

            stagingSURCP.setProcedureText(parser.getProcedureText().getString());
            stagingSURCP.setModifierText(parser.getModifierText().getString());
            stagingSURCP.setPrimaryProcedureIndicator(parser.getPrimaryProcedureIndicator().getInt());
            if (!BartsCsvHelper.isEmptyOrIsZero(parser.getSurgeonPersonnelId())) {
                stagingSURCP.setSurgeonPersonnelId(parser.getSurgeonPersonnelId().getInt());
            }

            CsvCell startCell = parser.getStartDateTime();
            if (!startCell.isEmpty()) {
                stagingSURCP.setDtStart(startCell.getDateTime());
            }

            CsvCell stopCell = parser.getStopDateTime();
            if (!stopCell.isEmpty()) {
                stagingSURCP.setDtStop(stopCell.getDateTime());
            }

            stagingSURCP.setWoundClassCode(parser.getWoundClassCode().getString());

            ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();
            auditWrapper.auditValue(parser.getSurgicalCaseId().getPublishedFileId(), parser.getSurgicalCaseId().getRecordNumber(), parser.getSurgicalCaseId().getColIndex(), "SurgicalCaseId");
            auditWrapper.auditValue(parser.getActiveIndicator().getPublishedFileId(), parser.getActiveIndicator().getRecordNumber(), parser.getActiveIndicator().getColIndex(), "ActiveInd");
            auditWrapper.auditValue(parser.getSurgeonPersonnelId().getPublishedFileId(), parser.getSurgeonPersonnelId().getRecordNumber(), parser.getSurgeonPersonnelId().getColIndex(), "SurgeonPersonnelId");
            auditWrapper.auditValue(parser.getSurgicalCaseId().getPublishedFileId(), parser.getSurgicalCaseId().getRecordNumber(), parser.getSurgicalCaseId().getColIndex(), "SurgicalCaseId");
            auditWrapper.auditValue(parser.getSurgicalCaseProcedureId().getPublishedFileId(), parser.getSurgicalCaseProcedureId().getRecordNumber(), parser.getSurgicalCaseProcedureId().getColIndex(), "SurgicalCaseProcedureId");
            auditWrapper.auditValue(parser.getProcedureCode().getPublishedFileId(), parser.getProcedureCode().getRecordNumber(), parser.getProcedureCode().getColIndex(), "ProcedureCode");
            stagingSURCP.setAudit(auditWrapper);
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SURCPPreTransformer.saveDataCallable(parser.getCurrentState(), stagingSURCP, serviceId));
    }

    private static class saveDataCallable extends AbstractCsvCallable {

        private StagingSURCP obj = null;
        private UUID serviceId;

        public saveDataCallable(CsvCurrentState parserState,
                                StagingSURCP obj,
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
