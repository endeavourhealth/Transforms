package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.SusEmergencyTail;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SusEmergencyTailPreTransformer extends CdsTailPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(SusEmergencyTailPreTransformer.class);
    //private static StagingCdsTailDalI repository = DalProvider.factoryStagingCdsTailDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {
        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                try {
                    processTailRecord((SusEmergencyTail)parser, csvHelper, BartsCsvHelper.SUS_RECORD_TYPE_EMERGENCY);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    /*private static void processRecord(SusEmergencyTail parser, BartsCsvHelper csvHelper) throws Exception{


        StagingCdsTail staging = new StagingCdsTail();
        String personId = parser.getPersonId().getString();
        if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
            return;
        }
        staging.setPersonId(parser.getPersonId().getInt());
        staging.setCdsUniqueIdentifier(parser.getCdsUniqueId().getString());
        staging.setExchangeId(parser.getExchangeId().toString());
        staging.setDtReceived(new Date());
        staging.setSusRecordType(BartsCsvHelper.SUS_RECORD_TYPE_OUTPATIENT);
        staging.setCdsUpdateType(parser.getCdsUpdateType().getInt());
        staging.setMrn(parser.getLocalPatientId().getString());
        staging.setNhsNumber(parser.getNhsNumber().getString());

        staging.setEncounterId(parser.getEncounterId().getInt());
        staging.setResponsibleHcpPersonnelId(parser.getResponsiblePersonnelId().getInt());


        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new SusEmergencyTailPreTransformer.saveDataCallable(parser.getCurrentState(), staging, serviceId));

    }


    private static class saveDataCallable extends AbstractCsvCallable {

        private StagingCdsTail obj = null;
        private UUID serviceId;

        public saveDataCallable(CsvCurrentState parserState,
                                StagingCdsTail obj,
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
    }*/
}


