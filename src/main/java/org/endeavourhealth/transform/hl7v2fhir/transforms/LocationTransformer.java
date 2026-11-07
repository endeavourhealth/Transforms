package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.primitive.ID;
import ca.uhn.hl7v2.model.v23.segment.PV1;
import org.endeavourhealth.core.database.dal.usermanager.caching.OrganisationCache;
import org.endeavourhealth.core.database.rdbms.datasharingmanager.models.OrganisationEntity;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.hl7v2fhir.helpers.AcuteHL7Helper;
import org.hl7.fhir.instance.model.Location;
import org.hl7.fhir.instance.model.ResourceType;

public class LocationTransformer {

    /**
     *
     * @param organizationBuilder
     * @return
     * @throws Exception
     */
    public static LocationBuilder createHL7LocationBuilder(FhirResourceFiler fhirResourceFiler, OrganizationBuilder organizationBuilder,String odsCode) throws Exception {
        LocationBuilder location = new LocationBuilder();

        OrganisationEntity org = OrganisationCache.getOrganisationDetailsFromOdsCode(odsCode);
        String name = org.getName();

        location.setId(odsCode);
        location.setStatus(Location.LocationStatus.ACTIVE);
        location.setName(name);
        location.setMode(Location.LocationMode.INSTANCE);
        location.setManagingOrganisation(AcuteHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
        fhirResourceFiler.saveAdminResource(null, location);

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
            location.setStatus(Location.LocationStatus.ACTIVE);
            location.setName(loc);
            location.setMode(Location.LocationMode.INSTANCE);

        }
        return location;
    }

    public static void savePatientLocation(FhirResourceFiler fhirResourceFiler, OrganizationBuilder organizationBuilder, PV1 pv1) throws Exception {
        //LocationPatientAssLoc
        LocationBuilder locationBuilderPatientAssLoc = null;
        locationBuilderPatientAssLoc = new LocationBuilder();
        String assignedPatientLoc = String.valueOf(pv1.getAssignedPatientLocation().getPointOfCare());
        if(assignedPatientLoc != null) {
            locationBuilderPatientAssLoc = LocationTransformer.transformPV1ToPatientAssignedLocation(pv1, locationBuilderPatientAssLoc);
            locationBuilderPatientAssLoc.setManagingOrganisation(AcuteHL7Helper.createReference(ResourceType.Organization, organizationBuilder.getResourceId()));
            fhirResourceFiler.saveAdminResource(null, locationBuilderPatientAssLoc);
        }
    }

}
