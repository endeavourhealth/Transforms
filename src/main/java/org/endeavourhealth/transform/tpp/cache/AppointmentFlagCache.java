package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.tpp.csv.transforms.appointment.AppointmentFlagsPojo;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.StaffMemberProfilePojo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class AppointmentFlagCache {
    // A simple HashMap with index key and a pojo class as a temporary cache
    private static final Logger LOG = LoggerFactory.getLogger(AppointmentFlagCache.class);

    private static HashMap<Long, List<AppointmentFlagsPojo>> AppointmentFlagsByAppointmentId = new HashMap<>();

    public static void addAppointmentFlagPojo(AppointmentFlagsPojo pojo) {
        Long key = pojo.getIDAppointment().getLong();
        if (AppointmentFlagsByAppointmentId.containsKey(key)) {
            AppointmentFlagsByAppointmentId.get(key).add(pojo);
        } else {
            List<AppointmentFlagsPojo> pojoList = new ArrayList<AppointmentFlagsPojo>();
            pojoList.add(pojo);
            AppointmentFlagsByAppointmentId.put(key, pojoList);
        }
    }

    public static List<AppointmentFlagsPojo> getStaffMemberProfilePojo(Long pojoKey) {
        return AppointmentFlagsByAppointmentId.get(pojoKey);
    }

    public static void removeAppointmentFlagPojo(AppointmentFlagsPojo pojo) {
        AppointmentFlagsByAppointmentId.remove(pojo.getIDAppointment());
    }

    public static boolean containsAppointmentId(Long apptId) {
        return (AppointmentFlagsByAppointmentId.containsKey(apptId));
    }

    public static int size() {
        return AppointmentFlagsByAppointmentId.size();
    }
}

