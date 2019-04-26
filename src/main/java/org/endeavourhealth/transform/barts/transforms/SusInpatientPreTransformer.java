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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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


        StagingCds stagingCds = new StagingCds();
        stagingCds.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
        stagingCds.setExchangeId(parser.getExchangeId().toString());
        stagingCds.setDtReceived(new Date());
        stagingCds.setCdsActivityDate(parser.getCdsActivityDate().getDate());
        stagingCds.setSusRecordType(csvHelper.SUS_RECORD_TYPE_INPATIENT);
        stagingCds.setCdsUpdateType(parser.getCdsUpdateType().getInt());
        stagingCds.setMrn(parser.getLocalPatientId().getString());
        stagingCds.setNhsNumber(parser.getNhsNumber().getString());
        stagingCds.setDateOfBirth(parser.getPersonBirthDate().getDate());
        String consultantStr = parser.getConsultantCode().getString();
        stagingCds.setConsultantCode(consultantStr);
        if (parser.getPrimaryProcedureOPCS().isEmpty()) {
            LOG.warn("No PrimaryProcedureOPCS Code for " +  parser.getCdsUniqueId());
            return;
        }
        String opcsCode = parser.getPrimaryProcedureOPCS().getString();
        opcsCode = TerminologyService.standardiseOpcs4Code(opcsCode);
        stagingCds.setPrimaryProcedureOpcsCode(opcsCode);
        stagingCds.setLookupProcedureOpcsTerm(TerminologyService.lookupOpcs4ProcedureName(opcsCode));
        String personnelIdStr = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, consultantStr);
        if (personnelIdStr!=null) {
            stagingCds.setLookupConsultantPersonnelId(Integer.parseInt(personnelIdStr));
        }
       stagingCds.setLookupPersonId(Integer.parseInt(csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID,
               parser.getLocalPatientId().getString())));

        UUID serviceId = csvHelper.getServiceId();


        // loop through proc codes. Primary and secondary first then the list
        //Primary
        stagingCds.setProcedureOpcsCode(opcsCode);
        stagingCds.setLookupProcedureOpcsTerm(TerminologyService.lookupOpcs4ProcedureName(opcsCode));
        stagingCds.setProcedureSeqNbr(1);
        if (parser.getPrimaryProcedureDate().isEmpty()) {
            LOG.warn("Missing primary date for " + parser.getCdsUniqueId());
            return;
        } //Empty primary dates seem to be when we also have missing primary code. So should no longer see these.
        stagingCds.setProcedureDate(parser.getPrimaryProcedureDate().getDate());
        stagingCds.setRecordChecksum(stagingCds.hashCode());
        csvHelper.submitToThreadPool(new SusInpatientPreTransformer.saveDataCallable(parser.getCurrentState(), stagingCds, serviceId));

        //Secondary
        if (!parser.getSecondaryProcedureOPCS().isEmpty()) {
            StagingCds stagingCds2 = stagingCds.clone();
            opcsCode = parser.getSecondaryProcedureOPCS().getString();
            stagingCds2.setProcedureOpcsCode(opcsCode);
            stagingCds2.setLookupProcedureOpcsTerm(TerminologyService.lookupOpcs4ProcedureName(opcsCode));
            stagingCds2.setProcedureSeqNbr(2);
            if (parser.getSecondaryProcedureDate().isEmpty()) {
                LOG.warn("Missing secondary date for " + parser.getCdsUniqueId());
                stagingCds2.setProcedureDate(parser.getPrimaryProcedureDate().getDate());
            } else {//TODO review handling of empty dates
                stagingCds2.setProcedureDate(parser.getSecondaryProcedureDate().getDate());
            }
                stagingCds2.setRecordChecksum(stagingCds.hashCode());
                csvHelper.submitToThreadPool(new SusInpatientPreTransformer.saveDataCallable(parser.getCurrentState(), stagingCds2, serviceId));

        }
        //Rest
        CsvCell otherProcedureOPCS = parser.getAdditionalecondaryProceduresOPCS();
        List<String> otherProcs = new ArrayList<>();
        List<String> otherDates = new ArrayList<>();
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        int seq = 3;
        for (String word : BartsSusHelper.splitEqually(otherProcedureOPCS.getString(),40)) {
            if (Strings.isNullOrEmpty(word)) {
                break;
            }
            StagingCds stagingCds3 = stagingCds.clone();
            String code = word.substring(0, 4);
            if (code.isEmpty()) {break;}
            if (word.length()>4) {
                String dateStr = word.substring(4,12);
                if (Strings.isNullOrEmpty(dateStr)) {
                    stagingCds3.setProcedureDate(parser.getPrimaryProcedureDate().getDate());
                } else {
                    Date date = dateFormat.parse(dateStr);
                    stagingCds3.setProcedureDate(date);
                }
            }
            stagingCds3.setProcedureOpcsCode(code);
            stagingCds3.setLookupProcedureOpcsTerm(TerminologyService.lookupOpcs4ProcedureName(code));
            stagingCds3.setProcedureSeqNbr(seq);
            stagingCds3.setRecordChecksum(stagingCds.hashCode());
            csvHelper.submitToThreadPool(new SusInpatientPreTransformer.saveDataCallable(parser.getCurrentState(), stagingCds3, serviceId));
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
                repository.save(obj, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}
