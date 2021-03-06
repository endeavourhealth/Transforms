package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.primitive.ID;
import ca.uhn.hl7v2.model.v23.datatype.XON;
import ca.uhn.hl7v2.model.v23.segment.PD1;
import ca.uhn.hl7v2.model.v23.segment.PV1;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(LocationTransformer.class);

    /**
     *
     * @param location
     * @return
     * @throws Exception
     */
    public static LocationBuilder transformPV1ToOrgLocation(LocationBuilder location) throws Exception {
        location.setId("Imperial College Healthcare NHS Trust");
        //location.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_LOCATION));
        location.setStatus(Location.LocationStatus.ACTIVE);
        location.setName("Imperial College Healthcare NHS Trust");
        //location.setDescription("Imperial College Healthcare NHS Trust");
        location.setMode(Location.LocationMode.INSTANCE);

        return location;
    }

    /**
     *
     * @param pv1
     * @param location
     * @return
     * @throws Exception
     */
    public static LocationBuilder transformPV1ToPatientAssignedLocation(PV1 pv1, LocationBuilder location) throws Exception {
        ID assignedPatientLoc = pv1.getAssignedPatientLocation().getPointOfCare();
        if(assignedPatientLoc.getValue() != null) {
            String loc= String.valueOf(assignedPatientLoc);
            location.setId(loc);
            //location.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_LOCATION));
            location.setStatus(Location.LocationStatus.ACTIVE);
            location.setName(loc);
            //location.setDescription(loc[1]+","+loc[2]);
            location.setMode(Location.LocationMode.INSTANCE);

        }
        return location;
    }



}
