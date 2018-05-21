package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AppointmentBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class AppointmentResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(AppointmentResourceCache.class);

    private static Map<Long, AppointmentBuilder> appointmentBuildersById = new HashMap<>();

    public static AppointmentBuilder getAppointmentBuilder(CsvCell appointmentIdCell,
                                                            TppCsvHelper csvHelper,
                                                            FhirResourceFiler fhirResourceFiler) throws Exception {

        AppointmentBuilder appointmentBuilder = appointmentBuildersById.get(appointmentIdCell.getLong());
        Appointment appointment = null;
        if (appointmentBuilder == null) {
            Resource apptResource = csvHelper.retrieveResource(appointmentIdCell.getString(), ResourceType.Appointment, fhirResourceFiler);
            if (apptResource != null && apptResource instanceof Appointment) {
                appointment = (Appointment) apptResource;
            }
            if (appointment == null) {
                //if the Appointment doesn't exist yet, create a new one
                appointmentBuilder = new AppointmentBuilder();
                appointmentBuilder.setId(appointmentIdCell.getString(), appointmentIdCell);
            } else {
                appointmentBuilder = new AppointmentBuilder(appointment);
            }

            appointmentBuildersById.put(appointmentIdCell.getLong(), appointmentBuilder);
        }
        return appointmentBuilder;
    }

    public static void fileAppointmentResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Long appointmentId: appointmentBuildersById.keySet()) {
            AppointmentBuilder appointmentBuilder = appointmentBuildersById.get(appointmentId);
            fhirResourceFiler.savePatientResource(null, appointmentBuilder);
        }

        //clear down as everything has been saved
        appointmentBuildersById.clear();
    }

    public static void clearAppointmentResourceCache() throws Exception {
        appointmentBuildersById.clear();
    }
}
