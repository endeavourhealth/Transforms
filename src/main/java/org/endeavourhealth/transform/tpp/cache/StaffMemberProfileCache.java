package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.StaffMemberProfilePojo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class StaffMemberProfileCache {
    // A simple HashMap with index key and a pojo class as a temporary cache
    private static final Logger LOG = LoggerFactory.getLogger(StaffMemberProfileCache.class);

    private static HashMap<Long, List<StaffMemberProfilePojo>> StaffMemberProfileByStaffId = new HashMap<>();

    public static void addStaffPojo(StaffMemberProfilePojo pojo) {
        Long key = pojo.getIDStaffMember();
        if (StaffMemberProfileByStaffId.containsKey(key)) {
            StaffMemberProfileByStaffId.get(key).add(pojo);
        } else {
            List<StaffMemberProfilePojo> pojoList = new ArrayList<StaffMemberProfilePojo>();
            pojoList.add(pojo);
            StaffMemberProfileByStaffId.put(key, pojoList);
        }
    }

    public static List<StaffMemberProfilePojo> getStaffMemberProfilePojo(Long pojoKey) {
        return StaffMemberProfileByStaffId.get(pojoKey);
    }

    public static void removeStaffPojo(StaffMemberProfilePojo pojo) {
        StaffMemberProfileByStaffId.remove(pojo.getIDStaffMember());
    }

    public static boolean containsStaffId(Long staffId) {
        return (StaffMemberProfileByStaffId.containsKey(staffId));
    }

    public int size() {
        return StaffMemberProfileByStaffId.size();
    }
}

