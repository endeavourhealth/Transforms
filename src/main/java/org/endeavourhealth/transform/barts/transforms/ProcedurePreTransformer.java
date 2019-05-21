package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingProcedureDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedure;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class ProcedurePreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedurePreTransformer.class);

    private static StagingProcedureDalI repository = DalProvider.factoryBartsStagingProcedureDalI();


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((org.endeavourhealth.transform.barts.schema.Procedure) parser, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(org.endeavourhealth.transform.barts.schema.Procedure parser, BartsCsvHelper csvHelper) throws Exception {


        // Observer not null conditions in DB
        CsvCell encounterCell = parser.getEncounterIdSanitised();

        StagingProcedure obj = new StagingProcedure();
        String personId = csvHelper.findPersonIdFromEncounterId(encounterCell);

        if (Strings.isNullOrEmpty(personId)) {
            TransformWarnings.log(LOG, csvHelper, "No person ID found for Procedure for encounter ID {}", encounterCell);
            return;
        }

        if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
            TransformWarnings.log(LOG, csvHelper, "Skipping Procedure with encounter ID {} as not part of filtered subset", encounterCell);
            return;
        }

        obj.setLookupPersonId(Integer.valueOf(personId));
        obj.setExchangeId(parser.getExchangeId().toString());
        obj.setDtReceived(csvHelper.getDataDate());
        obj.setMrn(parser.getMrn().getString());

        CsvCell nhsNumberCell = parser.getNhsNumberSanitised();
        obj.setNhsNumber(nhsNumberCell.getString());

        CsvCell dobCell = parser.getDateOfBirth();
        obj.setDateOfBirth(dobCell.getDate());

        obj.setEncounterId(encounterCell.getInt()); // Remember encounter ids from Procedure have a trailing .00
        obj.setConsultant(parser.getConsultant().getString());

        CsvCell dtCell = parser.getProcedureDateTime();

        //taken out until we see this happening. Not happy with the possibility of silently ignoring rows until we know we need to
        /*if (dtCell.isEmpty()) {
            return;
        }*/

        obj.setProcDtTm(BartsCsvHelper.parseDate(dtCell));
        obj.setUpdatedBy(parser.getUpdatedBy().getString());
        obj.setCreateDtTm(parser.getCreateDateTime().getDate());
        obj.setComments(parser.getComment().getString());

        //proceCdType is either "OPCS4" or "SNOMED CT". Snomed description Ids are used.
        String procCdType = parser.getProcedureCodeType().getString();
        obj.setProcCdType(procCdType);

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

        obj.setProcCd(procCd);
        obj.setProcTerm(procTerm);

        obj.setWard(parser.getWard().getString());
        obj.setSite(parser.getSite().getString());

        String consultantPersonnelId = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, parser.getConsultant().getString());
        if (!Strings.isNullOrEmpty(consultantPersonnelId)) {
            obj.setLookupConsultantPersonnelId(Integer.valueOf(consultantPersonnelId));
        }

        String recordedByPersonnelId = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID,parser.getUpdatedBy().getString());
        if (!Strings.isNullOrEmpty(recordedByPersonnelId)) {
            obj.setLookupRecordedByPersonnelId(Integer.valueOf(recordedByPersonnelId));
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new ProcedurePreTransformer.saveDataCallable(parser.getCurrentState(), obj, serviceId));
    }

    private static class saveDataCallable extends AbstractCsvCallable {

        private StagingProcedure obj = null;
        private UUID serviceId;

        public saveDataCallable(CsvCurrentState parserState,
                                StagingProcedure obj,
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
