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

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class SURCCPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SURCCPreTransformer.class);

    private static StagingSURCCDalI repository = DalProvider.factoryStagingSURCCDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {
        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((SURCC) parser, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    private static void processRecord(SURCC parser, BartsCsvHelper csvHelper) throws Exception {

        StagingSURCC stagingSURCC = new StagingSURCC();
        stagingSURCC.setExchangeId(parser.getExchangeId().toString());
        stagingSURCC.setDtReceived(new Date());

        stagingSURCC.setSurgicalCaseId(parser.getSurgicalCaseId().getInt());
        stagingSURCC.setDtExtract(parser.getExtractDateTime().getDate());

        boolean activeInd = parser.getActiveIndicator().getIntAsBoolean();
        stagingSURCC.setActiveInd(activeInd);

        if (activeInd) {
            String personId = parser.getPersonId().getString();
            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                return;
            }

            //if no start or end, then the surgery hasn't happened yet, so skip
            CsvCell startCell = parser.getSurgeryStartDtTm();
            CsvCell stopCell = parser.getSurgeryStopDtTm();
            if (startCell.isEmpty()
                    && stopCell.isEmpty()) {
                return;
            }

            //cache the person ID for our case ID, so the CURCP parser can look it up
            csvHelper.savePersonIdFromSurccId(parser.getSurgicalCaseId().getInt(), personId);

            stagingSURCC.setPersonId(Integer.parseInt(personId));
            stagingSURCC.setEncounterId(parser.getEncounterId().getInt());

            CsvCell cancelledCell = parser.getCancelledDateTime();
            if (cancelledCell.isEmpty()) {
                stagingSURCC.setDtCancelled(cancelledCell.getDateTime());
            }

            stagingSURCC.setInstitutionCode(parser.getInstitutionCode().getString());
            stagingSURCC.setDepartmentCode(parser.getDepartmentCode().getString());
            stagingSURCC.setSurgicalAreaCode(parser.getSurgicalAreaCode().getString());
            stagingSURCC.setTheatreNumberCode(parser.getTheatreNumberCode().getString());
            stagingSURCC.setSpecialtyCode(parser.getSpecialityCode().getString());

            ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();
            auditWrapper.auditValue(parser.getSurgicalCaseId().getPublishedFileId(), parser.getSurgicalCaseId().getRecordNumber(), parser.getSurgicalCaseId().getColIndex(), "SurgicalCaseId");
            auditWrapper.auditValue(parser.getActiveIndicator().getPublishedFileId(), parser.getActiveIndicator().getRecordNumber(), parser.getActiveIndicator().getColIndex(), "ActiveInd");
            auditWrapper.auditValue(parser.getPersonId().getPublishedFileId(), parser.getPersonId().getRecordNumber(), parser.getPersonId().getColIndex(), "PersonId");
            auditWrapper.auditValue(parser.getEncounterId().getPublishedFileId(), parser.getEncounterId().getRecordNumber(), parser.getEncounterId().getColIndex(), "EncounterId");
            auditWrapper.auditValue(parser.getInstitutionCode().getPublishedFileId(), parser.getInstitutionCode().getRecordNumber(), parser.getInstitutionCode().getColIndex(), "InstitutionCode");
            auditWrapper.auditValue(parser.getDepartmentCode().getPublishedFileId(), parser.getDepartmentCode().getRecordNumber(), parser.getDepartmentCode().getColIndex(), "DepartmentCode");
            auditWrapper.auditValue(parser.getTheatreNumberCode().getPublishedFileId(), parser.getTheatreNumberCode().getRecordNumber(), parser.getTheatreNumberCode().getColIndex(), "TheatreNumberCode");
            auditWrapper.auditValue(parser.getSpecialityCode().getPublishedFileId(), parser.getSpecialityCode().getRecordNumber(), parser.getSpecialityCode().getColIndex(), "SpecialtyCode");
            stagingSURCC.setAudit(auditWrapper);
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SURCCPreTransformer.saveDataCallable(parser.getCurrentState(), stagingSURCC, serviceId));
    }

    private static class saveDataCallable extends AbstractCsvCallable {

        private StagingSURCC obj = null;
        private UUID serviceId;

        public saveDataCallable(CsvCurrentState parserState,
                                StagingSURCC obj,
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
