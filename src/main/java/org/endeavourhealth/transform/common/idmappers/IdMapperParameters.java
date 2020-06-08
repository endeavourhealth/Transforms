package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Parameters;
import org.hl7.fhir.instance.model.Resource;

import java.util.Map;
import java.util.Set;

public class IdMapperParameters extends BaseIdMapper {



    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Parameters parameters = (Parameters)resource;

        //super.addCommonResourceReferences(parameters, referenceValues);
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        Parameters parameters = (Parameters)resource;

        //super.mapCommonResourceFields(parameters, mappings, failForMissingMappings);
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        return null;
    }
}
