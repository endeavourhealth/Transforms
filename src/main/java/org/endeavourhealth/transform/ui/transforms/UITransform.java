package org.endeavourhealth.transform.ui.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.ui.helpers.ReferencedResources;
import org.endeavourhealth.transform.ui.models.resources.UIResource;
import org.endeavourhealth.transform.ui.models.resources.admin.UIPatient;
import org.endeavourhealth.transform.ui.models.resources.clinicial.*;
import org.endeavourhealth.transform.ui.models.types.UIService;
import org.endeavourhealth.transform.ui.transforms.admin.UIPatientTransform;
import org.endeavourhealth.transform.ui.transforms.clinical.*;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class UITransform {
    public static UIPatient transformPatient(UUID serviceId, UUID systemId, Patient patient, ReferencedResources referencedResources) {
        return UIPatientTransform.transform(serviceId, systemId, patient, referencedResources);
    }

    public static <T extends UIResource> UIClinicalTransform getClinicalTransformer(Class<T> resourceType) {

        if (resourceType == UICondition.class)
            return new UIConditionTransform();
        else if (resourceType == UIProblem.class)
            return new UIProblemTransform();
        else if (resourceType == UIEncounter.class)
            return new UIEncounterTransform();
        else if (resourceType == UIObservation.class)
            return new UIObservationTransform();
        else if (resourceType == UIAllergyIntolerance.class)
            return new UIAllergyIntoleranceTransform();
        else if (resourceType == UIImmunisation.class)
            return new UIImmunisationTransform();
        else if (resourceType == UIProcedure.class)
            return new UIProcedureTransform();
        else if (resourceType == UIDiary.class)
            return new UIDiaryTransform();
        else if (resourceType == UIMedicationStatement.class)
            return new UIMedicationStatementTransform();
        else if (resourceType == UIMedicationOrder.class)
        		return new UIMedicationOrderTransform();
        else if (resourceType == UIFamilyMemberHistory.class)
            return new UIFamilyMemberHistoryTransform();
        else if (resourceType == UIEpisodeOfCare.class)
            return new UIEpisodeOfCareTransform();
        else if (resourceType == UIDiagnosticReport.class)
            return new UIDiagnosticReportTransform();
        else if (resourceType == UIDiagnosticOrder.class)
            return new UIDiagnosticOrderTransform();
        else if (resourceType == UIReferral.class)
            return new UIReferralTransform();
        else if (resourceType == UISpecimen.class)
            return new UISpecimenTransform();

        throw new NotImplementedException(resourceType.getSimpleName());
    }

    public static List<UIService> transformServices(List<Service> services) throws TransformException {
        List<UIService> result = new ArrayList<>();

        for (Service service : services) {
                result.addAll(transformService(service));
        }

        return result;
    }

    private static List<UIService> transformService(Service service) throws TransformException {
        List<UIService> result = new ArrayList<>();

        try {
            if (service == null)
                return result;

            if (StringUtils.isBlank(service.getEndpoints()))
                return result;

            ObjectMapper mapper = new ObjectMapper();
            JsonNode endpoints = mapper.readValue(service.getEndpoints(), JsonNode.class);

            if (endpoints.isArray())
                for (final JsonNode endpoint : endpoints) {
                    if (endpoint.has("systemUuid")) {
                        String systemUuidString = endpoint.get("systemUuid").asText().replace("\"", "");

                        if (StringUtils.isNotBlank(systemUuidString))
                            result.add(new UIService()
                                .setName(service.getName())
                                .setLocalIdentifier(service.getLocalId())
                                .setServiceId(service.getId())
                                .setSystemId(UUID.fromString(systemUuidString))
                            );
                    }
                }

            return result;

        } catch (Exception e) {
            throw new TransformException("Transform exception occurred - please see inner exception", e);
        }
    }

}
