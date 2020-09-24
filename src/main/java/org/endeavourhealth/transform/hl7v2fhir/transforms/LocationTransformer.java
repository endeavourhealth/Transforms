package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.ST;
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

        AddressBuilder addressBuilder = new AddressBuilder(location);
        addressBuilder.setUse(Address.AddressUse.WORK);

        /*addressBuilder.setCity(nameOfTownCell.getString());
        addressBuilder.setDistrict(nameOfCountyCell.getString());
        addressBuilder.setPostcode(fullPostCodeCell.getString());*/

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
        ST assignedPatientLoc = pv1.getAssignedPatientLocation().getLocationType();
        if(assignedPatientLoc.getValue() != null) {
            String loc[] = String.valueOf(assignedPatientLoc).split(",");
            location.setId(loc[0]);
            //location.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_LOCATION));
            location.setStatus(Location.LocationStatus.ACTIVE);
            location.setName(loc[0]);
            //location.setDescription(loc[1]+","+loc[2]);
            location.setMode(Location.LocationMode.INSTANCE);

            AddressBuilder addressBuilder = new AddressBuilder(location);
            addressBuilder.setUse(Address.AddressUse.WORK);
            addressBuilder.addLine(String.valueOf(assignedPatientLoc));

            /*addressBuilder.setCity(nameOfTownCell.getString());
            addressBuilder.setDistrict(nameOfCountyCell.getString());
            addressBuilder.setPostcode(fullPostCodeCell.getString());*/
        }
        return location;
    }

}
