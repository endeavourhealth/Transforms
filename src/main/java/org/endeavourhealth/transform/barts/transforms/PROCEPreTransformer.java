package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingPROCEDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingPROCE;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedure;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.PROCE;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class PROCEPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PROCEPreTransformer.class);

    private static StagingPROCEDalI repository = DalProvider.factoryBartsStagingPROCEDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        List<StagingPROCE> batch = new ArrayList<>();

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {
                //no try/catch here, since any failure here means we don't want to continue
                processRecord((PROCE) parser, fhirResourceFiler, csvHelper, batch);
            }
        }

        saveBatch(batch, true, csvHelper);
    }

    private static void saveBatch(List<StagingPROCE> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new PROCEPreTransformer.saveDataCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    public static void processRecord(PROCE parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper, List<StagingPROCE> batch) throws Exception {

        StagingPROCE stagingPROCE = new StagingPROCE();

        stagingPROCE.setActiveInd(parser.getActiveIndicator().getIntAsBoolean());
        stagingPROCE.setExchangeId(parser.getExchangeId().toString());
        stagingPROCE.setDtReceived(csvHelper.getDataDate());

        CsvCell procedureIdCell = parser.getProcedureID();
        stagingPROCE.setProcedureId(procedureIdCell.getInt());

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(procedureIdCell.getPublishedFileId(), procedureIdCell.getRecordNumber());
        stagingPROCE.setAudit(audit);

        boolean activeInd = parser.getActiveIndicator().getIntAsBoolean();
        stagingPROCE.setActiveInd(activeInd);

        //only set additional values if active
        if (activeInd) {

            CsvCell encounterIdCell = parser.getEncounterId();

            String personId = csvHelper.findPersonIdFromEncounterId(encounterIdCell);
            if (Strings.isNullOrEmpty(personId)) {
                TransformWarnings.log(LOG, csvHelper, "No person ID found for PROCE {}", procedureIdCell);
                return;
            }

            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                TransformWarnings.log(LOG, csvHelper, "Skipping PROCE {} as not part of filtered subset", procedureIdCell);
                return;
            }

            stagingPROCE.setLookupPersonId(Integer.valueOf(personId));

            //TYPE_MILLENNIUM_PERSON_ID_TO_MRN
            String mrn = csvHelper.getInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personId);
            if (mrn != null) {
                stagingPROCE.setLookupMrn(mrn);
            } else {
                TransformWarnings.log(LOG, csvHelper, "PROCE {} has no MRN from lookup for person {}", procedureIdCell, personId);
            }

            //LOG.debug("Processing procedure " + procedureIdCell.getString());

            stagingPROCE.setEncounterId(encounterIdCell.getInt());

            //DAB-121 enhancement to derive the responsiblePersonnelId from the encounter internal map
            String responsiblePersonnelId = csvHelper.findResponsiblePersonnelIdFromEncounterId(encounterIdCell);
            if (!Strings.isNullOrEmpty(responsiblePersonnelId)) {
                stagingPROCE.setLookupResponsiblePersonnelId(Integer.valueOf(responsiblePersonnelId));
            }

            CsvCell sliceCell = parser.getEncounterSliceID();
            if (!sliceCell.isEmpty()) {
                stagingPROCE.setEncounterSliceId(sliceCell.getInt());
            }

            CsvCell dateCell = parser.getProcedureDateTime();
            //LOG.debug("Date cell has [" + dateCell.getString() + "]");
            Date procDate = BartsCsvHelper.parseDate(dateCell);
            //explicitly been told that if a PROCE record has no date, then skip it
            //LOG.debug("Parsed procedure date = " + procDate);
            if (procDate == null) {
                //LOG.debug("Skipping this procedure");
                return;
            }
            stagingPROCE.setProcedureDtTm(procDate);

            CsvCell conceptCell = parser.getConceptCodeIdentifier();
            String codeId = csvHelper.getProcedureOrDiagnosisConceptCode(conceptCell);
            if (Strings.isNullOrEmpty(codeId)) {
                //a very small number of PROCE records have no code (e.g. procedure ID 427665727) but we can't do anything with it
                TransformWarnings.log(LOG, csvHelper, "Procedure {} has no concept code so will be skipped", procedureIdCell);
                return;
                //throw new Exception("Missing procedure code for procid " + procId);
            }
            stagingPROCE.setProcedureCode(codeId);

            String procTerm;
            String codeType = csvHelper.getProcedureOrDiagnosisConceptCodeType(parser.getConceptCodeIdentifier());
            stagingPROCE.setProcedureType(codeType);
            if (codeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
                procTerm = TerminologyService.lookupOpcs4ProcedureName(codeId);
                if (Strings.isNullOrEmpty(procTerm)) {
                    throw new Exception("Failed to find term for OPCS-4 code " + codeId);
                }

            } else if (codeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED)) {
                procTerm = TerminologyService.lookupSnomedTerm(codeId);
                if (Strings.isNullOrEmpty(procTerm)) {
                    throw new Exception("Failed to find term for Snomed code " + codeId);
                }

            } else if (codeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_ICD_10)) {
                //only a tiny number of these
                procTerm = TerminologyService.lookupIcd10CodeDescription(codeId);
                if (Strings.isNullOrEmpty(procTerm)) {
                    throw new Exception("Failed to find term for ICD 10 code " + codeId);
                }

            } else if (codeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_HRG)) {
                TransformWarnings.log(LOG, csvHelper, "PROCE record {} has HRG code in concept cell {}", procedureIdCell, conceptCell);
                return;

            } else {
                throw new Exception("Unexpected code type " + codeType);
            }

            stagingPROCE.setProcedureTerm(procTerm);

            CsvCell sequenceNumberCell = parser.getSequenceNumber();
            //there are a small number of PROCE records with a zero sequence number, so ignore it
            if (!BartsCsvHelper.isEmptyOrIsZero(sequenceNumberCell)) {
                stagingPROCE.setProcedureSeqNo(sequenceNumberCell.getInt());
            }

            //LOG.debug("Adding to thread thing with checksum " + stagingPROCE.hashCode());
        }

        batch.add(stagingPROCE);
        saveBatch(batch, false, csvHelper);
    }

    private static class saveDataCallable implements Callable {

        private List<StagingPROCE> objs = null;
        private UUID serviceId;

        public saveDataCallable(List<StagingPROCE> objs, UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.savePROCEs(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}


