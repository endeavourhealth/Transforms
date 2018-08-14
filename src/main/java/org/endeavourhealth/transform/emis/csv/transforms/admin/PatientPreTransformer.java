package org.endeavourhealth.transform.emis.csv.transforms.admin;

import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class PatientPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {


        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        try {
            AbstractCsvParser parser = parsers.get(Patient.class);
            while (parser.nextRecord()) {

                try {
                    processLine((Patient)parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    private static void processLine(Patient parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper,
                                    String version) throws Exception {

        CsvCell patientGuidCell = parser.getPatientGuid();
        String patientGuid = patientGuidCell.getString();
        CsvCurrentState parserState = parser.getCurrentState();

        PreCreateEdsPatientIdTask task = new PreCreateEdsPatientIdTask(patientGuid, fhirResourceFiler, parserState);
        csvHelper.submitToThreadPool(task);
    }


    private static void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since the first exception is always most relevant
        ThreadPoolError first = errors.get(0);
        PreCreateEdsPatientIdTask callable = (PreCreateEdsPatientIdTask)first.getCallable();
        Throwable exception = first.getException();
        CsvCurrentState parserState = callable.getParserState();
        throw new TransformException(parserState.toString(), exception);
    }

    static class PreCreateEdsPatientIdTask implements Callable {

        private String sourceEmisPatientGuid;
        private FhirResourceFiler fhirResourceFiler;
        private CsvCurrentState parserState;

        public PreCreateEdsPatientIdTask(String sourceEmisPatientGuid,
                          FhirResourceFiler fhirResourceFiler,
                          CsvCurrentState parserState) {

            this.sourceEmisPatientGuid = sourceEmisPatientGuid;
            this.fhirResourceFiler = fhirResourceFiler;
            this.parserState = parserState;
        }

        @Override
        public Object call() throws Exception {
            try {

                //just make the call into the ID helper, which will create and cache or just cache, so no creation is needed
                //when we get to the point of creating and saving resources
                IdHelper.getOrCreateEdsResourceIdString(fhirResourceFiler.getServiceId(), ResourceType.Patient, sourceEmisPatientGuid);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }

        public CsvCurrentState getParserState() {
            return parserState;
        }
    }
}
