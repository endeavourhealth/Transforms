package org.endeavourhealth.transform.adastra.transforms.admin;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.hl7.fhir.instance.model.Meta;

import java.util.ArrayList;
import java.util.List;

import static org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper.uniqueIdMapper;

public class LocationTransform {

    public static void transform(AdastraCaseDataExport caseReport, FhirResourceFiler fhirResourceFiler) throws Exception {

        if (caseReport.getPatient().getGpRegistration().getSurgeryNationalCode() != null) {
            String orgCode = caseReport.getPatient().getGpRegistration().getSurgeryNationalCode();
            List<String> locations = new ArrayList<>();

            if (caseReport.getLatestAppointment() != null && caseReport.getLatestAppointment().getLocation() != null) {
                createLocation(caseReport.getLatestAppointment().getLocation(), orgCode, fhirResourceFiler);
                locations.add(caseReport.getLatestAppointment().getLocation());
            }

            for (AdastraCaseDataExport.Consultation con : caseReport.getConsultation()) {
                // A new location name so add a new location
                if (!locations.contains(con.getLocation())) {
                    createLocation(con.getLocation(), orgCode, fhirResourceFiler);
                }
            }
        }
    }

    public static void createLocation(String locationText, String orgCode, FhirResourceFiler fhirResourceFiler) throws Exception {

        org.hl7.fhir.instance.model.Location fhirLocation = new org.hl7.fhir.instance.model.Location();
        fhirLocation.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_LOCATION));

        fhirLocation.setId(orgCode + ":" + locationText);
        fhirLocation.setManagingOrganization(AdastraHelper.createOrganisationReference(orgCode));
        uniqueIdMapper.put(locationText, fhirLocation.getId());

        fhirLocation.setName(locationText);
        fhirLocation.setType(CodeableConceptHelper.createCodeableConcept(locationText));

        fhirResourceFiler.saveAdminResource(null, fhirLocation);

    }
}
