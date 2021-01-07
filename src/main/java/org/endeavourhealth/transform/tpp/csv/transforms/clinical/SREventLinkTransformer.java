package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SREventLink;
import org.endeavourhealth.transform.tpp.csv.transforms.appointment.SRVisitTransformer;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SREventLinkTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SREventLinkTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SREventLink.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    processRecord((SREventLink) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void processRecord(SREventLink parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell removedData = parser.getRemovedData();
        if (removedData != null //column not always present
                && removedData.getIntAsBoolean()) {
            return;
        }

        CsvCell eventIdCell = parser.getIDEvent();
        if (!eventIdCell.isEmpty()) {

            CsvCell appointmentId = parser.getIDAppointment();
            if (!TppCsvHelper.isEmptyOrNegative(appointmentId)) {

                //for FHIR Appointments we just use the SRAppointment ID as the unique ID
                String srcId = appointmentId.getString();
                csvHelper.cacheNewEncounterAppointmentLink(eventIdCell, srcId, ResourceType.Appointment);
            }

            CsvCell visitIdCell = parser.getIDVisit();
            if (!TppCsvHelper.isEmptyOrNegative(visitIdCell)) {

                //SRVisit records are transformed into FHIR Appoitments and add a prefix to the unique ID to make it clear
                String visitIdUnique = SRVisitTransformer.VISIT_ID_PREFIX + visitIdCell.getString();
                csvHelper.cacheNewEncounterAppointmentLink(eventIdCell, visitIdUnique, ResourceType.Appointment);
            }
        }
    }

}
