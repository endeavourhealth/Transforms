package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
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
                //no try/catch as records in this file aren't independent and can't be re-processed on their own

                //can't filter on patients here, as we don't have a person ID cell, so this is done lower down
                processRecord((SURCP) parser, csvHelper);
            }
        }
    }

    private static void processRecord(SURCP parser, BartsCsvHelper csvHelper) throws Exception {

        StagingSURCP stagingSURCP = new StagingSURCP();
        stagingSURCP.setExchangeId(parser.getExchangeId().toString());
        stagingSURCP.setDtReceived(csvHelper.getDataDate());

        CsvCell procedureIdCell = parser.getSurgicalCaseProcedureId();
        stagingSURCP.setSurgicalCaseProcedureId(procedureIdCell.getInt());

        CsvCell extractedDateCell = parser.getExtractDateTime();
        stagingSURCP.setDtExtract(BartsCsvHelper.parseDate(extractedDateCell));

        boolean activeInd = parser.getActiveIndicator().getIntAsBoolean();
        stagingSURCP.setActiveInd(activeInd);

        LOG.trace("Processing SURCP procedure ID " + procedureIdCell.getString() + " active ind = " + activeInd);

        if (activeInd) {

            CsvCell surgicalCaseIdCell = parser.getSurgicalCaseId();
            stagingSURCP.setSurgicalCaseId(surgicalCaseIdCell.getInt());

            LOG.trace("Surgical case ID " + surgicalCaseIdCell.getString());

            //we don't strictly need the person ID for loading the staging table, but we do if
            //we want to filter to a subset of patients
            String personIdStr = csvHelper.findPersonIdFromSurgicalCaseId(surgicalCaseIdCell);
            if (Strings.isNullOrEmpty(personIdStr)) {
                TransformWarnings.log(LOG, csvHelper, "No person ID found for SURCP {}", procedureIdCell);
                return;
            }

            if (!csvHelper.processRecordFilteringOnPatientId(personIdStr)) {
                TransformWarnings.log(LOG, csvHelper, "Skipping SURCP {} as not part of filtered subset", procedureIdCell);
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

            CsvCell surgeonIdCell = parser.getSurgeonPersonnelId();
            if (!BartsCsvHelper.isEmptyOrIsZero(surgeonIdCell)) {
                stagingSURCP.setSurgeonPersonnelId(surgeonIdCell.getInt());
            }

            CsvCell startCell = parser.getStartDateTime();
            if (!startCell.isEmpty()) {
                Date d = BartsCsvHelper.parseDate(startCell);
                stagingSURCP.setDtStart(d);
            }

            CsvCell stopCell = parser.getStopDateTime();
            if (!stopCell.isEmpty()) {
                Date d = BartsCsvHelper.parseDate(stopCell);
                stagingSURCP.setDtStop(d);
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
        LOG.trace("Added SURCP procedure ID " + procedureIdCell.getString() + " to thread pool");
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
                LOG.trace("Saved SURCP procedure ID " + obj.getSurgicalCaseProcedureId());

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}
