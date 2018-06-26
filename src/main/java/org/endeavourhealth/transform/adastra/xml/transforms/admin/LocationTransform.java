package org.endeavourhealth.transform.adastra.xml.transforms.admin;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.transform.adastra.AdastraXmlHelper;
import org.endeavourhealth.transform.adastra.xml.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.hl7.fhir.instance.model.Meta;

import java.util.ArrayList;
import java.util.List;

import static org.endeavourhealth.transform.adastra.AdastraXmlHelper.uniqueIdMapper;

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
        fhirLocation.setManagingOrganization(AdastraXmlHelper.createOrganisationReference(orgCode));
        uniqueIdMapper.put(locationText, fhirLocation.getId());

        fhirLocation.setName(locationText);
        fhirLocation.setType(CodeableConceptHelper.createCodeableConcept(locationText));

        fhirResourceFiler.saveAdminResource(null, new LocationBuilder(fhirLocation));

    }
}
