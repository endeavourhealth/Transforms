package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AppointmentBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.AppointmentResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.appointment.SRAppointmentFlags;

import java.util.Map;

public class SRAppointmentFlagsTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRAppointmentFlags.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRAppointmentFlags)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createResource(SRAppointmentFlags parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell appointmentId = parser.getIDAppointment();

        AppointmentBuilder appointmentBuilder
                = AppointmentResourceCache.getAppointmentBuilder(appointmentId, csvHelper, fhirResourceFiler);

        //flags could range from Interpretor Required to Transport Booked so add the detail to the appointment comments
        CsvCell appointmentFlag = parser.getFlag();
        if (!appointmentFlag.isEmpty() && appointmentFlag.getLong()>0) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(appointmentFlag.getLong());
            String flagMapping = tppMappingRef.getMappedTerm();
            if (!Strings.isNullOrEmpty(flagMapping)) {
                appointmentBuilder.setComments(flagMapping);
            }
        }
    }
}
