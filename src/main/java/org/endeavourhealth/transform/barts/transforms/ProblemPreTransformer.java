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

import java.util.List;
import java.util.UUID;

public class ProblemPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProblemPreTransformer.class);

    private static StagingProblemDalI repository = DalProvider.factoryBartsStagingProblemDalI();


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((org.endeavourhealth.transform.barts.schema.Problem) parser, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(org.endeavourhealth.transform.barts.schema.Problem parser, BartsCsvHelper csvHelper) throws Exception {

        StagingProblem obj = new StagingProblem();

        CsvCell problemIdCell = parser.getProblemId();
        CsvCell personIdCell = parser.getPersonIdSanitised();
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

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(problemIdCell.getPublishedFileId(), problemIdCell.getRecordNumber());
        obj.setAudit(audit);

        CsvCell onsetDtCell = parser.getOnsetDate();
        obj.setOnsetDtTm(BartsCsvHelper.parseDate(onsetDtCell));

        //vocab is either "UK Ed Subset" or "SNOMED CT". Snomed description Ids are used.
        String vocab = parser.getVocabulary().getString();
        obj.setVocab(vocab);

        String probTerm = "";
        String probCode = parser.getProblemCode().getString();

        if (vocab.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_ICD_10)) {
            probTerm = TerminologyService.lookupIcd10CodeDescription(probCode);
            if (Strings.isNullOrEmpty(probTerm)) {
                throw new Exception("Failed to find term for ICD-10 code [" + probCode + "]");
            }
        } else if (vocab.equalsIgnoreCase("OPCS4")) {

            String term = TerminologyService.lookupOpcs4ProcedureName(probCode);
            if (Strings.isNullOrEmpty(term)) {
                throw new Exception("Failed to find term for OPCS-4 code [" + probCode + "]");
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

        } else if (vocab.equalsIgnoreCase("Cerner")) {
            // in this file, Cerner VOCAB doesn't seem to mean it refers to the CVREF file, so don't make any
            // attempt to look up an official term and just use the original problem term text

            probTerm = parser.getProblem().getString();
        } else {
            throw new Exception("Unexpected coding scheme vocab " + vocab);
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

        CsvCell statusCell = parser.getStatusLifecycle();
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

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new ProblemPreTransformer.saveDataCallable(parser.getCurrentState(), obj, serviceId));
    }

    private static class saveDataCallable extends AbstractCsvCallable {

        private StagingProblem obj = null;
        private UUID serviceId;

        public saveDataCallable(CsvCurrentState parserState,
                                StagingProblem obj,
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
