package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.ST;
import ca.uhn.hl7v2.model.v23.segment.PV1;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Location;
import org.hl7.fhir.instance.model.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(LocationTransformer.class);

    /**
     *
     * @param pv1
     * @param location
     * @return
     * @throws Exception
     */
    public static Location transformPV1ToOrgLocation(PV1 pv1, Location location) throws Exception {
        location.setId("Imperial College Healthcare NHS Trust");
        location.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_LOCATION));
        location.setStatus(Location.LocationStatus.ACTIVE);
        location.setName("Imperial College Healthcare NHS Trust");
        location.setDescription("Imperial College Healthcare NHS Trust");
        location.setMode(Location.LocationMode.INSTANCE);

        Address address = new Address();
        address.setUse(Address.AddressUse.WORK);
        location.setAddress(address);

        return location;
    }

    /**
     *
     * @param pv1
     * @param location
     * @return
     * @throws Exception
     */
    public static Location transformPV1ToPatientAssignedLocation(PV1 pv1, Location location) throws Exception {
        ST assignedPatientLoc = pv1.getAssignedPatientLocation().getLocationType();
        String loc[] = String.valueOf(pv1.getAssignedPatientLocation().getLocationType()).split(",");
        location.setId(loc[0]);
        location.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_LOCATION));
        location.setStatus(Location.LocationStatus.ACTIVE);
        location.setName(loc[0]);
        location.setDescription(loc[1]+","+loc[2]);
        location.setMode(Location.LocationMode.INSTANCE);

        Address address = new Address();
        address.setUse(Address.AddressUse.WORK);
        address.addLine(String.valueOf(pv1.getAssignedPatientLocation().getLocationType()));
        //address.setCity();
        location.setAddress(address);

        return location;
    }

}
