package org.endeavourhealth.transform.tpp.cache;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AppointmentBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.transforms.appointment.AppointmentFlagsPojo;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

// A simple HashMap with index key and a pojo class as a temporary cache
public class AppointmentFlagCache {

    private static final Logger LOG = LoggerFactory.getLogger(AppointmentFlagCache.class);

    private HashMap<Long, List<AppointmentFlagsPojo>> appointmentFlagsByAppointmentId = new HashMap<>();

    public void addAppointmentFlagPojo(AppointmentFlagsPojo pojo) {
        Long key = pojo.getIdAppointment().getLong();
        if (appointmentFlagsByAppointmentId.containsKey(key)) {
            appointmentFlagsByAppointmentId.get(key).add(pojo);
        } else {
            List<AppointmentFlagsPojo> pojoList = new ArrayList<AppointmentFlagsPojo>();
            pojoList.add(pojo);
            appointmentFlagsByAppointmentId.put(key, pojoList);
        }
    }

    public List<AppointmentFlagsPojo> getAndRemoveFlagsForAppointmentId(Long pojoKey) {
        return appointmentFlagsByAppointmentId.remove(pojoKey);
    }

    /**
     * we assume that if we ever get any SRAppointmentFlags, then we'll also always get a record
     * in SRAppointment. This fn is called after the two are transformed to ensure that was the case
     * and that there are no flags left over that didn't have a corresponding record in SRAppointment
     */
    public void processRemainingFlags(TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {
        for (Long appointmentId: appointmentFlagsByAppointmentId.keySet()) {
            List<AppointmentFlagsPojo> flags = appointmentFlagsByAppointmentId.get(appointmentId);

            Appointment appointment = (Appointment)csvHelper.retrieveResource("" + appointmentId, ResourceType.Appointment);
            if (appointment == null) {
                continue;
            }

            AppointmentBuilder appointmentBuilder = new AppointmentBuilder(appointment);
            applyFlagsToAppointment(csvHelper, appointmentBuilder, flags);

            fhirResourceFiler.savePatientResource(null, false, appointmentBuilder);
        }
    }

    public static void applyFlagsToAppointment(TppCsvHelper csvHelper, AppointmentBuilder appointmentBuilder, List<AppointmentFlagsPojo> pojoList) throws Exception {

        for (AppointmentFlagsPojo pojo : pojoList) {
            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(pojo.getFlag());
            if (tppMappingRef != null) {
                String flagMapping = tppMappingRef.getMappedTerm();
                if (!Strings.isNullOrEmpty(flagMapping)) {
                    String comment = ((org.hl7.fhir.instance.model.Appointment) appointmentBuilder.getResource()).getComment();
                    if (!Strings.isNullOrEmpty(comment) && !comment.contains(flagMapping)) {
                        appointmentBuilder.setComments(comment + "," + flagMapping);
                    } else {
                        appointmentBuilder.setComments(flagMapping);
                    }
                }
            }
        }

    }
}

