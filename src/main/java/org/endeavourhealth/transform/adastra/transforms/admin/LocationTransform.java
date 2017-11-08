package org.endeavourhealth.transform.adastra.transforms.admin;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Resource;

import java.util.ArrayList;
import java.util.List;

import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.guidMapper;

public class LocationTransform {

    public static void transform(AdastraCaseDataExport caseReport, List<Resource> resources) {

        List<String> locations = new ArrayList<>();

        createLocation(caseReport.getLatestAppointment().getLocation(), resources);
        locations.add(caseReport.getLatestAppointment().getLocation());

        for (AdastraCaseDataExport.Consultation con : caseReport.getConsultation()) {
            // A new location name so add a new location
            if (!locations.contains(con.getLocation())) {
                createLocation(con.getLocation(), resources);
            }
        }
    }

    public static void createLocation(String locationText, List<Resource> resources) {

        //TODO work out the org id
        String orgId = "Adastra";

        org.hl7.fhir.instance.model.Location fhirLocation = new org.hl7.fhir.instance.model.Location();
        fhirLocation.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_LOCATION));

        AdastraHelper.setUniqueLocationId(fhirLocation, orgId, locationText);
        guidMapper.put(locationText, fhirLocation.getId());

        fhirLocation.setName(locationText);
        fhirLocation.setType(CodeableConceptHelper.createCodeableConcept(locationText));

        resources.add(fhirLocation);

    }
}
