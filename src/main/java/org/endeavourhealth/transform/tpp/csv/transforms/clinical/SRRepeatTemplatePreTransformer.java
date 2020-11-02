package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRRepeatTemplate;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRRepeatTemplatePreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRRepeatTemplatePreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRRepeatTemplate.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    processRecord((SRRepeatTemplate) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(SRRepeatTemplate parser, FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper) {
        //if this record is deleted, skip it
        CsvCell removedCell = parser.getRemovedData();
        if (removedCell != null && removedCell.getIntAsBoolean()) {
            return;
        }

        CsvCell id = parser.getRowIdentifier();
        String localId = SRRepeatTemplateTransformer.REPEAT_TEMPLATE_ID_PREFIX + id.getString();

        CsvCell eventIdCell = parser.getIDEvent();
        if (!eventIdCell.isEmpty()) {
            csvHelper.cacheNewConsultationChildRelationship(eventIdCell, localId, ResourceType.MedicationStatement);
        }
    }
}

