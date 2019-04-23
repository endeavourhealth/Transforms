package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingSURCCDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingSURCC;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.SURCC;
import org.endeavourhealth.transform.common.AbstractCsvCallable;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
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
        stagingSURCC.setDTReceived(new Date());

        stagingSURCC.setSurgicalCaseId(parser.getSurgicalCaseId().getInt());
        stagingSURCC.setDTExtract(parser.getExtractDateTime().getDate());

        boolean activeInd = parser.getActiveIndicator().getIntAsBoolean();
        stagingSURCC.setActiveInd(activeInd);

        if (activeInd) {
            stagingSURCC.setPersonId(parser.getPersonId().getInt());
            stagingSURCC.setEncounterId(parser.getEncounterId().getInt());
            if (stagingSURCC.getDTCancelled()!=null) {
                stagingSURCC.setDTCancelled(parser.getCancelledDateTime().getDate());
            }
            stagingSURCC.setInstitutionCode(parser.getInstitutionCode().getString());
            stagingSURCC.setDepartmentCode(parser.getDepartmentCode().getString());
            stagingSURCC.setSurgicalAreaCode(parser.getSurgicalAreaCode().getString());
            stagingSURCC.setTheatreNumberCode(parser.getTheatreNumberCode().getString());

            stagingSURCC.setRecordChecksum(stagingSURCC.hashCode());
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
                repository.save(obj, serviceId);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}
