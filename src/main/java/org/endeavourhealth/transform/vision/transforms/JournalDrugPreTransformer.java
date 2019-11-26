package org.endeavourhealth.transform.vision.transforms;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Journal;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;

import static org.endeavourhealth.transform.vision.transforms.JournalTransformer.getTargetResourceType;

public class JournalDrugPreTransformer {

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        AbstractCsvParser parser = parsers.get(Journal.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    processLine((Journal) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }
        }
    }


    private static void processLine(Journal parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    VisionCsvHelper csvHelper) throws Exception {

        CsvCell actionCell = parser.getAction();
        if (actionCell.getString().equalsIgnoreCase("D")) {
            return;
        }

        ResourceType resourceType = getTargetResourceType(parser);
        //all we are interested in are Drug records (non issues)
        if (resourceType == ResourceType.MedicationStatement) {
            CsvCell patientID = parser.getPatientID();
            CsvCell drugRecordID = parser.getObservationID();

            //cache the observation IDs of Drug records (not Issues), so  that we know what is a Drug Record
            //when we run the observation pre and main transformers, i.e. for linking and deriving medications issues
            csvHelper.cacheDrugRecordGuid(patientID, drugRecordID);
        }
    }
}