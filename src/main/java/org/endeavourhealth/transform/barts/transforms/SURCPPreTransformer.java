package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingSURCPDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingSURCP;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.SURCP;
import org.endeavourhealth.transform.common.AbstractCsvCallable;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class SURCPPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SURCPPreTransformer.class);

    private static StagingSURCPDalI repository = DalProvider.factoryStagingSURCPDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {
        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((SURCP) parser, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    private static void processRecord(SURCP parser, BartsCsvHelper csvHelper) throws Exception {

        StagingSURCP stagingSURCP = new StagingSURCP();
        stagingSURCP.setExchangeId(parser.getExchangeId().toString());
        stagingSURCP.setDTReceived(new Date());

        stagingSURCP.setSurgicalCaseProcedureId(parser.getSurgicalCaseProcedureId().getInt());

        boolean activeInd = parser.getActiveIndicator().getIntAsBoolean();
        stagingSURCP.setActiveInd(activeInd);

        if (activeInd) {
            stagingSURCP.setDTExtract(parser.getExtractDateTime().getDate());
            stagingSURCP.setSurgicalCaseId(parser.getSurgicalCaseId().getInt());
            stagingSURCP.setProcedureCode(parser.getProcedureCode().getInt());
            stagingSURCP.setProcedureText(parser.getProcedureText().getString());
            stagingSURCP.setModifierText(parser.getModifierText().getString());
            stagingSURCP.setPrimaryProcedureIndicator(parser.getPrimaryProcedureIndicator().getInt());
            stagingSURCP.setSurgeonPersonnelId(parser.getSurgeonPersonnelId().getInt());
            stagingSURCP.setDTStart(parser.getStartDateTime().getDate());
            stagingSURCP.setDTStop(parser.getStopDateTime().getDate());
            stagingSURCP.setWoundClassCode(parser.getWoundClassCode().getString());

            stagingSURCP.setRecordChecksum(stagingSURCP.hashCode());
        }

        UUID serviceId = csvHelper.getServiceId();

        csvHelper.submitToThreadPool(new SURCPPreTransformer.saveDataCallable(parser.getCurrentState(), stagingSURCP, serviceId));
    }

    private static class saveDataCallable extends AbstractCsvCallable {

        private StagingSURCP obj = null;
        private UUID serviceId;

        public saveDataCallable(CsvCurrentState parserState,
                                StagingSURCP obj,
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
