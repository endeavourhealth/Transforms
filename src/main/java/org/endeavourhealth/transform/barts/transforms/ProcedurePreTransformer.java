package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingProcedureDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedure;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingSURCP;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class ProcedurePreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedurePreTransformer.class);

    private static StagingProcedureDalI repository = DalProvider.factoryBartsStagingProcedureDalI();


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        List<StagingProcedure> batch = new ArrayList<>();

        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                processRecord((org.endeavourhealth.transform.barts.schema.Procedure) parser, csvHelper, batch);
            }
        }

        saveBatch(batch, true, csvHelper);
    }

    private static void saveBatch(List<StagingProcedure> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new ProcedurePreTransformer.saveDataCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    private static void processRecord(org.endeavourhealth.transform.barts.schema.Procedure parser, BartsCsvHelper csvHelper, List<StagingProcedure> batch) throws Exception {

        // Observer not null conditions in DB
        CsvCell encounterCell = parser.getEncounterIdSanitised();
        String personId = csvHelper.findPersonIdFromEncounterId(encounterCell);

        if (Strings.isNullOrEmpty(personId)) {
            TransformWarnings.log(LOG, csvHelper, "No person ID found for Procedure for encounter ID {}", encounterCell);
            return;
        }

        if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
            TransformWarnings.log(LOG, csvHelper, "Skipping Procedure with encounter ID {} as not part of filtered subset", encounterCell);
            return;
        }

        StagingProcedure stagingObj = new StagingProcedure();

        stagingObj.setLookupPersonId(Integer.valueOf(personId));
        stagingObj.setExchangeId(parser.getExchangeId().toString());
        stagingObj.setDtReceived(csvHelper.getDataDate());

        CsvCell mrnCell = parser.getMrn();
        stagingObj.setMrn(mrnCell.getString());

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(mrnCell.getPublishedFileId(), mrnCell.getRecordNumber());
        stagingObj.setAudit(audit);

        CsvCell nhsNumberCell = parser.getNhsNumberSanitised();
        stagingObj.setNhsNumber(nhsNumberCell.getString());

        CsvCell dobCell = parser.getDateOfBirth();
        stagingObj.setDateOfBirth(dobCell.getDate());

        stagingObj.setEncounterId(encounterCell.getInt()); // Remember encounter ids from Procedure have a trailing .00
        stagingObj.setConsultant(parser.getConsultant().getString());

        CsvCell dtCell = parser.getProcedureDateTime();

        //taken out until we see this happening. Not happy with the possibility of silently ignoring rows until we know we need to
        /*if (dtCell.isEmpty()) {
            return;
        }*/

        stagingObj.setProcDtTm(BartsCsvHelper.parseDate(dtCell));
        stagingObj.setUpdatedBy(parser.getUpdatedBy().getString());
        stagingObj.setCreateDtTm(parser.getCreateDateTime().getDate());
        stagingObj.setComments(parser.getComment().getString());

        //proceCdType is either "OPCS4" or "SNOMED CT". Snomed description Ids are used.
        String procCdType = parser.getProcedureCodeType().getString();
        stagingObj.setProcCdType(procCdType);

        String procTerm = "";
        String procCd = parser.getProcedureCode().getString();

        if (procCdType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
            procTerm = TerminologyService.lookupOpcs4ProcedureName(procCd);
            if (Strings.isNullOrEmpty(procTerm)) {
                throw new Exception("Failed to find term for OPCS-4 code [" + procCd + "]");
            }

        } else if (procCdType.equals(BartsCsvHelper.CODE_TYPE_SNOMED_CT)) {
            //note, although the column says it's Snomed, it's actually a Snomed description ID, not a concept ID
            SnomedCode snomedCode = TerminologyService.lookupSnomedConceptForDescriptionId(procCd);
            if (snomedCode == null) {
                throw new Exception("Failed to find term for Snomed description ID [" + procCd + "]");
            }
            procTerm = snomedCode.getTerm();
            procCd = snomedCode.getConceptCode();  //update the code to be an actual Snomed ConceptId

        } else {
            throw new Exception("Unexpected coding scheme " + procCdType);
        }

        stagingObj.setProcCd(procCd);
        stagingObj.setProcTerm(procTerm);

        stagingObj.setWard(parser.getWard().getString());
        stagingObj.setSite(parser.getSite().getString());

        String consultantStr  = parser.getConsultant().getString();
        String consultantPersonnelId = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, consultantStr);
        if (!Strings.isNullOrEmpty(consultantPersonnelId)) {
            stagingObj.setLookupConsultantPersonnelId(Integer.valueOf(consultantPersonnelId));
        } else {
            //Make sure name is formatted as "surname , names"
                String newConsultantStr = formatName(consultantStr);
                consultantPersonnelId = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, newConsultantStr);

            if (!Strings.isNullOrEmpty(consultantPersonnelId)) {
                stagingObj.setLookupConsultantPersonnelId(Integer.valueOf(consultantPersonnelId));
            }
        }

        String recordedByPersonnelId = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID,parser.getUpdatedBy().getString().replace(" ,",","));
        if (!Strings.isNullOrEmpty(recordedByPersonnelId)) {
            stagingObj.setLookupRecordedByPersonnelId(Integer.valueOf(recordedByPersonnelId));
        }

        batch.add(stagingObj);
        saveBatch(batch, false, csvHelper);
    }

    private static String formatName(String consultantStr) {
        int commaposn = consultantStr.indexOf(",");
        if (!Character.isWhitespace(consultantStr.charAt(commaposn-1))) {
            consultantStr = consultantStr.substring(0, commaposn) + " " + consultantStr.substring(commaposn);
        }
        commaposn = consultantStr.indexOf(",");
        if (!Character.isWhitespace(consultantStr.charAt(commaposn+1))) {
            consultantStr = consultantStr.substring(0, commaposn+1) + " " + consultantStr.substring(commaposn+1);
        }
        return consultantStr;
    }

    private static class saveDataCallable implements Callable {

        private List<StagingProcedure> objs = null;
        private UUID serviceId;

        public saveDataCallable(List<StagingProcedure> objs, UUID serviceId) {
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

}
