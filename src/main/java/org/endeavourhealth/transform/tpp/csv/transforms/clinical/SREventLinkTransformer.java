package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SREventLink;
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
        while (parser.nextRecord()) {

            try {
                createResource((SREventLink) parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(SREventLink parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell patientId = parser.getIDPatient();

        CsvCell appointmentId = parser.getIDAppointment();
        CsvCell visitId = parser.getIDVisit();

        CsvCell eventLinkId = parser.getIDEvent();
        if (!eventLinkId.isEmpty()) {

            if (!appointmentId.isEmpty()) {
                csvHelper.cacheNewEncounterAppointmentMap(eventLinkId,
                        patientId,
                        appointmentId,
                        ResourceType.Appointment);
            }

            if (!visitId.isEmpty()) {
                csvHelper.cacheNewConsultationChildRelationship(eventLinkId,
                        patientId,
                        visitId,
                        ResourceType.Encounter);
            }
        }

    }

}
