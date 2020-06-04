package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.List;

public class ContainedParametersBuilder {

    private static final String CONTAINED_PARAMETERS_ID = "Additional";

    private HasContainedParametersI parentBuilder;

    public ContainedParametersBuilder(HasContainedParametersI parentBuilder) {
        this.parentBuilder = parentBuilder;
    }

    public void addParameter(String name, String value, CsvCell... sourceCells) {

        addParameter(name, new StringType(value), sourceCells);
    }

    public void addParameter(String name, Type value, CsvCell... sourceCells) {

        DomainResource resource = parentBuilder.getResource();
        Parameters containedParameters = getOrCreateContainedParameters();

        Parameters.ParametersParameterComponent entry = containedParameters.addParameter();

        entry.setName(name);
        entry.setValue(value);

        int paramIndex = resource.getContained().indexOf(containedParameters);
        int entryIndex = containedParameters.getParameter().indexOf(entry);
        parentBuilder.auditValue("contained[" + paramIndex + "].parameter[" + entryIndex + "]", sourceCells);
    }

    public void removeContainedParameters() {

        DomainResource resource = parentBuilder.getResource();
        Parameters containedParameters = getOrCreateContainedParameters();

        //remove any audits we've created for the Contained Parameters
        int paramIndex = resource.getContained().indexOf(containedParameters);
        parentBuilder.getAuditWrapper().removeAudit("contained[" + paramIndex + "]");

        resource.getContained().remove(containedParameters);

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

    public List<Parameters.ParametersParameterComponent> getContainedParametersComponents() {

        Parameters parameters = getContainedParameters();

        if (parameters == null
                || !parameters.hasParameter()) {
            return null;
        }
        return parameters.getParameter();
    }
}
