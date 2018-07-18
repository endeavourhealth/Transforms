package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.tpp.csv.transforms.appointment.AppointmentFlagsPojo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class AppointmentFlagCache {
    // A simple HashMap with index key and a pojo class as a temporary cache
    private static final Logger LOG = LoggerFactory.getLogger(AppointmentFlagCache.class);

    private static HashMap<Long, List<AppointmentFlagsPojo>> appointmentFlagsByAppointmentId = new HashMap<>();

    public static void addAppointmentFlagPojo(AppointmentFlagsPojo pojo) {
        Long key = pojo.getIdAppointment().getLong();
        if (appointmentFlagsByAppointmentId.containsKey(key)) {
            appointmentFlagsByAppointmentId.get(key).add(pojo);
        } else {
            List<AppointmentFlagsPojo> pojoList = new ArrayList<AppointmentFlagsPojo>();
            pojoList.add(pojo);
            appointmentFlagsByAppointmentId.put(key, pojoList);
        }
    }

    public static List<AppointmentFlagsPojo> getFlagsForAppointmentId(Long pojoKey) {
        return appointmentFlagsByAppointmentId.get(pojoKey);
    }

    public static void removeAppointmentFlagPojo(AppointmentFlagsPojo pojo) {
        appointmentFlagsByAppointmentId.remove(pojo.getIdAppointment());
    }

    public static void removeFlagsByAppointmentId(Long apptKey) {
        appointmentFlagsByAppointmentId.remove(apptKey);
    }

    public static boolean containsAppointmentId(Long apptId) {
        return (appointmentFlagsByAppointmentId.containsKey(apptId));
    }

    public static int size() {
        return appointmentFlagsByAppointmentId.size();
    }

    public static void clear() {
        LOG.info("Appointment flag cache will be cleared of " + size() + " records.");
        appointmentFlagsByAppointmentId.clear();}
}

