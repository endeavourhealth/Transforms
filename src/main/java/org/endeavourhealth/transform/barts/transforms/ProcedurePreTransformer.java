package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingProcedureDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedure;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
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

        StagingProcedure obj = new StagingProcedure();
        obj.setExchangeId(parser.getExchangeId().toString());
        obj.setDateReceived(new Date());
        obj.setMrn(parser.getMrn().getString());
        obj.setNhsNumber(parser.getNHSNo());
        obj.setDateOfBirth(parser.getDOB());
        obj.setEncounterId(parser.getEncounterId().getInt()); // Remember encounter ids from Procedure have a trailing .00
        obj.setConsultant(parser.getConsultant().getString());
        Date procDate = csvHelper.parseDate(parser.getProcedureDateTime());
        if (procDate==null ) {
            return;
        }
        obj.setProcDtTm(procDate);
        obj.setUpdatedBy(parser.getUpdatedBy().getString());
        obj.setCreateDtTm(parser.getCreateDateTime().getDate());
        obj.setFreeTextComment(parser.getComment().getString());

        //proceCdType is either "OPCS4" or "SNOMED CT". Snomed description Ids are used.
        String procCdType = parser.getProcedureCodeType().getString();
        obj.setProcCdType(procCdType);

        String procTerm = "";
        String procCd = parser.getProcedureCode().getString();

        if (procCdType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
            procTerm = TerminologyService.lookupOpcs4ProcedureName(procCd);
        } else {
            SnomedCode snomedCode = TerminologyService.lookupSnomedConceptForDescriptionId(procCd);
            if (snomedCode != null) {
                procTerm = snomedCode.getTerm();
                procCd = snomedCode.getConceptCode();  //update the code to be an actual Snomed ConceptId
            } else {
                throw new TransformException("Unable to match Snomed Description Id [" + procCd + "]");
            }
        }

        obj.setProcCd(procCd);
        obj.setProcTerm(procTerm);

        String personId = csvHelper.findPersonIdFromEncounterId(parser.getEncounterId());
        obj.setPersonId(personId);
        obj.setWard(parser.getWard().getString());
        obj.setSite(parser.getSite().getString());
        obj.setLookupPersonId(personId);
        String consultantStr = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, parser.getConsultant().getString());
        if (consultantStr!=null) {
            obj.setLookupConsultantPersonnelId(Integer.parseInt(consultantStr));
        }
        String recordedBy = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID,parser.getUpdatedBy().getString());
        if (recordedBy!=null) {
            obj.setLookuprecordedByPersonnelId(Integer.parseInt(recordedBy));
        }
        obj.setCheckSum();

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
