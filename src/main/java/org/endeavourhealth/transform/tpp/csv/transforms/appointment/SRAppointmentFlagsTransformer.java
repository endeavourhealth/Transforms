package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AppointmentBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.AppointmentFlagCache;
import org.endeavourhealth.transform.tpp.cache.AppointmentResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.appointment.SRAppointmentFlags;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.StaffMemberProfilePojo;

import java.util.Map;

public class SRAppointmentFlagsTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRAppointmentFlags.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRAppointmentFlags) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
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
        apptFlagPojo.setIDAppointment(apptFlagPojo.getIDAppointment());

        CsvCell appointmentFlagsId = parser.getRowIdentifier();
        apptFlagPojo.setRowIdentifier(appointmentFlagsId);

        CsvCell orgId = parser.getIDOrganisationVisibleTo();
        if (!orgId.isEmpty()) {
            apptFlagPojo.setIDOrganisationVisibleTo(orgId);
        }

        CsvCell flag = parser.getFlag();
        if (!flag.isEmpty()) {
            apptFlagPojo.setFlag(flag);
        }

        CsvCell removed = parser.getRemovedData();
        if (!removed.isEmpty()) {
            apptFlagPojo.setRemovedData(removed);
        }

        AppointmentFlagCache.addAppointmentFlagPojo(apptFlagPojo);

        }
    }

