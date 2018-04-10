package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRCode;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;

public class SRCodePreTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        AbstractCsvParser parser = parsers.get(SRCode.class);
        while (parser.nextRecord()) {

            try {
                processLine((SRCode)parser, csvHelper);
            } catch (Exception ex) {
                throw new TransformException(parser.getCurrentState().toString(), ex);
            }
        }
    }


    private static void processLine(SRCode parser, TppCsvHelper csvHelper) throws Exception {

        ResourceType resourceType = SRCodeTransformer.getTargetResourceType(parser, csvHelper);

        CsvCell observationId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        // code linked consultation / encounter Id
        CsvCell eventLinkId = parser.getIDEvent();
        if (!eventLinkId.isEmpty()) {

            csvHelper.cacheNewConsultationChildRelationship(eventLinkId,
                    patientId,
                    observationId,
                    resourceType);
        }
    }
}


