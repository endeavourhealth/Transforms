package org.endeavourhealth.transform.vision.transforms;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Journal;

import java.util.Map;

public class JournalProblemPreTransformer {

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Journal.class);
        while (parser.nextRecord()) {

            try {
                processLine((Journal) parser, fhirResourceFiler, csvHelper, version);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void processLine(Journal parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       VisionCsvHelper csvHelper,
                                       String version) throws Exception {

        //all this pre-transformer does is cache the observation IDs of problems, so
        //that we know what is a problem when we run process the problem links pre-transformer
        if (parser.getSubset().getString().equalsIgnoreCase("P")) {
            CsvCell patientID = parser.getPatientID();
            CsvCell observationID = parser.getObservationID();
            String readCode = parser.getReadCode().getString();
            csvHelper.cacheProblemObservationGuid(patientID, observationID, readCode);
        }
    }
}
