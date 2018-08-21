package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.appointment.SRAppointmentFlags;

import java.util.Map;

public class SRAppointmentFlagsTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRAppointmentFlags.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                //no try/catch here because record level exceptions in this transform
                //will affect subsequent files, so exceptions must be allowed to be throw up
                createResource((SRAppointmentFlags) parser, fhirResourceFiler, csvHelper);
            }
        }
    }

    private static void createResource(SRAppointmentFlags parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell appointmentId = parser.getIDAppointment();
        if (appointmentId.isEmpty()) {
            return;
        }

        AppointmentFlagsPojo apptFlagPojo = new AppointmentFlagsPojo();
        apptFlagPojo.setIdAppointment(appointmentId);

        CsvCell appointmentFlagsId = parser.getRowIdentifier();
        apptFlagPojo.setRowIdentifier(appointmentFlagsId);

        CsvCell orgId = parser.getIDOrganisationVisibleTo();
        if (!orgId.isEmpty()) {
            apptFlagPojo.setIdOrganisationVisibleTo(orgId);
        }

        CsvCell flag = parser.getFlag();
        if (!flag.isEmpty()) {
            apptFlagPojo.setFlag(flag);
        }

        CsvCell removed = parser.getRemovedData();
        if ((removed != null) && !removed.isEmpty()) {
            apptFlagPojo.setRemovedData(removed);
        }

        csvHelper.getAppointmentFlagCache().addAppointmentFlagPojo(apptFlagPojo);

    }
}

