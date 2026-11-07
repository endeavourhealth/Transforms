package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Parameters;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperParameters extends BaseIdMapper {


    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Parameters parameters = (Parameters)resource;

        if (parameters.hasParameter()) {

            Parameters.ParametersParameterComponent parameter = parameters.getParameter().get(0);
            //check for value as some HL7v2 message don't have these set
            if (parameter.hasValue()) {
                String parameterType = parameter.getValue().getClass().getSimpleName();

                if (parameterType.equalsIgnoreCase("Reference")) {

                    Reference referenceValue = (Reference) parameter.getValue();
                    //SD-950 - only map/process Patient references. HL7v2 uses a practitioner Id reference which fails this
                    ReferenceComponents comps = ReferenceHelper.getReferenceComponents(referenceValue);
                    if (null != comps) {
                        ResourceType resourceType = comps.getResourceType();
                        if (resourceType.equals(ResourceType.Patient)) {
                            super.addReference(referenceValue, referenceValues);
                        }
                    }
                }
            }
        }
        //super.addCommonResourceReferences(parameters, referenceValues);
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        Parameters parameters = (Parameters)resource;

        if (parameters.hasParameter()) {

            Parameters.ParametersParameterComponent parameter = parameters.getParameter().get(0);
            //check for value as some HL7v2 message don't have these set
            if (parameter.hasValue()) {
                String parameterType = parameter.getValue().getClass().getSimpleName();

                if (parameterType.equalsIgnoreCase("Reference")) {

                    Reference referenceValue = (Reference) parameter.getValue();
                    //SD-950 - only map/process Patient references. HL7v2 uses a practitioner Id reference which fails this
                    ReferenceComponents comps = ReferenceHelper.getReferenceComponents(referenceValue);
                    if (null != comps) {
                        ResourceType resourceType = comps.getResourceType();
                        if (resourceType.equals(ResourceType.Patient)) {
                            super.mapReference(referenceValue, mappings, failForMissingMappings);
                        }
                    }
                }
            }
        }
        //super.mapCommonResourceFields(parameters, mappings, failForMissingMappings);
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        return null;
    }
}
