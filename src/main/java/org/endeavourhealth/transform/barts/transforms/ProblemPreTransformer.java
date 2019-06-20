package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingProblemDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProblem;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class ProblemPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProblemPreTransformer.class);

    private static StagingProblemDalI repository = DalProvider.factoryBartsStagingProblemDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        List<StagingProblem> batch = new ArrayList<>();

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {
                //no try/catch here, since any failure here means we don't want to continue
                processRecord((org.endeavourhealth.transform.barts.schema.Problem) parser, csvHelper, batch);
            }
        }

        saveBatch(batch, true, csvHelper);

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void saveBatch(List<StagingProblem> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new ProblemPreTransformer.saveDataCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    private static void processRecord(org.endeavourhealth.transform.barts.schema.Problem parser, BartsCsvHelper csvHelper, List<StagingProblem> batch) throws Exception {

        StagingProblem obj = new StagingProblem();

        CsvCell problemIdCell = parser.getProblemIdSanitised();   // .00 suffix removed
        CsvCell personIdCell = parser.getPersonIdSanitised();     // .00 suffix removed
        String personId = personIdCell.getString();

        if (Strings.isNullOrEmpty(personId)) {
            TransformWarnings.log(LOG, csvHelper, "No person ID found for Problem ID {}", problemIdCell);
            return;
        }

        if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
            TransformWarnings.log(LOG, csvHelper, "Skipping Problem with problem ID {} as not part of filtered subset", problemIdCell);
            return;
        }

        obj.setExchangeId(parser.getExchangeId().toString());
        obj.setDtReceived(csvHelper.getDataDate());
        obj.setMrn(parser.getMrn().getString());
        obj.setUpdatedBy(parser.getUpdatedBy().getString());

        obj.setProblemId(problemIdCell.getInt());
        obj.setPersonId(personIdCell.getInt());

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(problemIdCell.getPublishedFileId(), problemIdCell.getRecordNumber());
        obj.setAudit(audit);

        CsvCell onsetDtCell = parser.getOnsetDate();
        obj.setOnsetDtTm(BartsCsvHelper.parseDate(onsetDtCell));

        CsvCell statusCell = parser.getStatusLifecycle();

        //vocab is either "UK Ed Subset" or "SNOMED CT". Snomed description Ids are used.
        String vocab = parser.getVocabulary().getString();
        obj.setVocab(vocab);

        String probTerm = "";
        String probCode = parser.getProblemCode().getString();

        if (vocab.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_ICD_10)
                || vocab.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_ICD_10_d)) {
            probTerm = TerminologyService.lookupIcd10CodeDescription(probCode);
            if (Strings.isNullOrEmpty(probTerm)) {
                throw new Exception("Failed to find term for ICD-10 code [" + probCode + "]");
            }
        } else if (vocab.equals(BartsCsvHelper.CODE_TYPE_SNOMED_CT) ||
                    vocab.equals(BartsCsvHelper.CODE_TYPE_UK_ED_SUBSET)) {
            //note, although the column says it's Snomed, it's actually a Snomed description ID, not a concept ID
            SnomedCode snomedCode = TerminologyService.lookupSnomedConceptForDescriptionId(probCode);
            if (snomedCode == null) {
                throw new Exception("Failed to find term for Snomed description ID [" + probCode + "]");
            }
            probTerm = snomedCode.getTerm();
            probCode = snomedCode.getConceptCode();  //update the code to be an actual Snomed ConceptId

        } else if (vocab.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_CERNER) ||
                    vocab.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_PATIENT_CARE)) {
            // in this file, Cerner VOCAB doesn't seem to mean it refers to the CVREF file, so don't make any
            // attempt to look up an official term and just use the original problem term text.  Also, Patient Care
            // records do not have codes so just use the term

            probTerm = parser.getProblem().getString();

        } else  if (vocab.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {

            //OPCS4 codes have been detected in Barts Problem files
            probTerm = TerminologyService.lookupOpcs4ProcedureName(probCode);
            if (Strings.isNullOrEmpty(probTerm)) {
                throw new Exception("Failed to find term for OPCS-4 code [" + probCode + "]");
            }
        } else {

            // only throw an exception if this is not a canceled (US spelling) record and the vocab is unrecognised
            if (!statusCell.isEmpty() && !statusCell.getString().equalsIgnoreCase("Canceled")) {
                throw new Exception("Unexpected coding scheme vocab " + vocab);
            }
        }

        obj.setProblemCd(probCode);
        obj.setProblemTerm(probTerm);

        CsvCell problemTxtCell = parser.getAnnotatedDisp();
        if (!problemTxtCell.isEmpty()) {
            obj.setProblemTxt(problemTxtCell.getString());
        }

        CsvCell classificationCell = parser.getClassification();
        if (!classificationCell.isEmpty()) {
            obj.setClassification(classificationCell.getString());
        }

        CsvCell confirmationCell = parser.getConfirmation();
        if (!confirmationCell.isEmpty()) {
            obj.setConfirmation(confirmationCell.getString());
        }

        CsvCell rankingCell = parser.getRanking();
        if (!rankingCell.isEmpty()) {
            obj.setRanking(rankingCell.getString());
        }

        // status cell is set before the coding processing
        if (!statusCell.isEmpty()) {
            obj.setProblemStatus(statusCell.getString());
        }

        CsvCell statusDateCell = parser.getStatusDate();
        if (!statusDateCell.isEmpty()) {
            obj.setProblemStatusDtTm(BartsCsvHelper.parseDate(statusDateCell));
        }

        CsvCell locationCell = parser.getOrgName();
        if (!locationCell.isEmpty()) {
            obj.setLocation(locationCell.getString());
        }

        String consultantPersonnelId
                = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, parser.getUpdatedBy().getString());
        if (!Strings.isNullOrEmpty(consultantPersonnelId)) {
            obj.setLookupConsultantPersonnelId(Integer.valueOf(consultantPersonnelId));
        }

        batch.add(obj);
        saveBatch(batch, false, csvHelper);
    }

    private static class saveDataCallable implements Callable {

        private List<StagingProblem> objs = null;
        private UUID serviceId;

        public saveDataCallable(List<StagingProblem> objs,
                                UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveProblems(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}