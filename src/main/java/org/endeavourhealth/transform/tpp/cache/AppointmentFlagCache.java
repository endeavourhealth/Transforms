package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.tpp.csv.transforms.appointment.AppointmentFlagsPojo;
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
    public void checkForRemainingFlags() throws Exception {
        if (!appointmentFlagsByAppointmentId.isEmpty()) {
            throw new TransformException("" + appointmentFlagsByAppointmentId.size() + " appointment flags didn't have records in SRAppointment");
        }
    }
}

