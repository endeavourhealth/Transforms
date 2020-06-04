package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.DomainResource;
import org.hl7.fhir.instance.model.Parameters;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;

import java.util.List;

public class ContainedParametersBuilder {

    private static final String CONTAINED_PARAMETERS_ID = "Additional";

    private HasContainedParametersI parentBuilder;

    public ContainedParametersBuilder(HasContainedParametersI parentBuilder) {
        this.parentBuilder = parentBuilder;
    }

    public void removeContainedParameters() {

        DomainResource resource = parentBuilder.getResource();
        Parameters parameters = getOrCreateContainedParameters();

        //remove any audits we've created for the Contained Parameters
        parentBuilder.getAuditWrapper().removeAudit("contained");

        resource.getContained().remove(parameters);

        //also remove the extension that points to the list
        String extensionUrl = parentBuilder.getContainedParametersExtensionUrl();
        ExtensionConverter.removeExtension(resource, extensionUrl);
    }

    private Parameters getContainedParameters() {

        DomainResource resource = parentBuilder.getResource();
        if (resource.hasContained()) {
            for (Resource contained: resource.getContained()) {
                if (contained.getId().equals(CONTAINED_PARAMETERS_ID)) {
                    return (Parameters) contained;
                }
            }
        }

        return null;
    }

    private Parameters getOrCreateContainedParameters() {

        DomainResource resource = parentBuilder.getResource();
        Parameters parameters = getContainedParameters();

        //if the Parameters wasn't there before, create and add it
        if (parameters == null) {
            parameters = new Parameters();
            parameters.setId(CONTAINED_PARAMETERS_ID);
            resource.getContained().add(parameters);
        }

        //add the extension, unless it's already there
        String extensionUrl = parentBuilder.getContainedParametersExtensionUrl();
        Reference parametersReference = ReferenceHelper.createInternalReference(CONTAINED_PARAMETERS_ID);
        ExtensionConverter.createOrUpdateExtension(resource, extensionUrl, parametersReference);

        return parameters;
    }

    public void addParameter(Parameters.ParametersParameterComponent parameterComponent, CsvCell... sourceCells) {
        DomainResource resource = parentBuilder.getResource();
        Parameters containedParameters = getContainedParameters();

        Parameters.ParametersParameterComponent entry = containedParameters.addParameter();

        entry.setName(parameterComponent.getName());
        entry.setValue(parameterComponent.getValue());

        int paramIndex = resource.getContained().indexOf(containedParameters);
        int entryIndex = containedParameters.getParameter().indexOf(entry);
        parentBuilder.auditValue("contained[" + paramIndex + "].parameter[" + entryIndex + "]", sourceCells);
    }

    public List<Parameters.ParametersParameterComponent> getContainedParametersItems() {
        Parameters parameters = getContainedParameters();

        if (parameters == null
                || !parameters.hasParameter()) {
            return null;
        }
        return parameters.getParameter();
    }
}
