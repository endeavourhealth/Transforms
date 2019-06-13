package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingSURCCDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingSURCC;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.SURCC;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class SURCCPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SURCCPreTransformer.class);

    private static StagingSURCCDalI repository = DalProvider.factoryStagingSURCCDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        List<StagingSURCC> batch = new ArrayList<>();

        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                //no try/catch as records in this file aren't independent and can't be re-processed on their own

                //filter on patients
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
                    TransformWarnings.log(LOG, csvHelper, "Skipping SURCC {} as not part of filtered subset", ((SURCC) parser).getSurgicalCaseId());
                    continue;
                }
                processRecord((SURCC) parser, csvHelper, batch);
            }
        }

        saveBatch(batch, true, csvHelper);
    }

    private static void saveBatch(List<StagingSURCC> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
            || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SURCCPreTransformer.saveDataCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    private static void processRecord(SURCC parser, BartsCsvHelper csvHelper, List<StagingSURCC> batch) throws Exception {

        StagingSURCC stagingSURCC = new StagingSURCC();
        stagingSURCC.setExchangeId(parser.getExchangeId().toString());
        stagingSURCC.setDtReceived(csvHelper.getDataDate());

        CsvCell surgicalCaseIdCell = parser.getSurgicalCaseId();
        stagingSURCC.setSurgicalCaseId(surgicalCaseIdCell.getInt());

        //audit that our staging object came from this file and record
        ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
        audit.auditRecord(surgicalCaseIdCell.getPublishedFileId(), surgicalCaseIdCell.getRecordNumber());
        stagingSURCC.setAudit(audit);

        CsvCell extractedDateCell = parser.getExtractDateTime();
        stagingSURCC.setDtExtract(BartsCsvHelper.parseDate(extractedDateCell));

        boolean activeInd = parser.getActiveIndicator().getIntAsBoolean();
        stagingSURCC.setActiveInd(activeInd);

        if (activeInd) {
            CsvCell personIdCell = parser.getPersonId();

            //if no start or end, then the surgery hasn't happened yet, so skip
            CsvCell startCell = parser.getSurgeryStartDtTm();
            if (!startCell.isEmpty()) {
                Date d = BartsCsvHelper.parseDate(startCell);
                stagingSURCC.setDtStart(d);
            }

            CsvCell stopCell = parser.getSurgeryStopDtTm();
            if (!stopCell.isEmpty()) {
                Date d = BartsCsvHelper.parseDate(stopCell);
                stagingSURCC.setDtStop(d);
            }

            //cache the person ID for our case ID, so the CURCP parser can look it up
            csvHelper.saveSurgicalCaseIdToPersonId(surgicalCaseIdCell, personIdCell);

            stagingSURCC.setPersonId(personIdCell.getInt());
            stagingSURCC.setEncounterId(parser.getEncounterId().getInt());

            CsvCell cancelledCell = parser.getCancelledDateTime();
            if (!cancelledCell.isEmpty()) {
                Date d = BartsCsvHelper.parseDate(cancelledCell);
                stagingSURCC.setDtCancelled(d);
            }

            stagingSURCC.setInstitutionCode(parser.getInstitutionCode().getString());
            stagingSURCC.setDepartmentCode(parser.getDepartmentCode().getString());
            stagingSURCC.setSurgicalAreaCode(parser.getSurgicalAreaCode().getString());
            stagingSURCC.setTheatreNumberCode(parser.getTheatreNumberCode().getString());
            stagingSURCC.setSpecialtyCode(parser.getSpecialityCode().getString());
        }

        batch.add(stagingSURCC);
        saveBatch(batch, false, csvHelper);
    }

    private static class saveDataCallable implements Callable {

        private List<StagingSURCC> objs = null;
        private UUID serviceId;

        public saveDataCallable(List<StagingSURCC> objs, UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveSURCCs(objs, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}
