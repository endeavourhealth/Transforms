package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.DiagnosticReport;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperDiagnosticReport extends BaseIdMapper {


    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {

        DiagnosticReport report = (DiagnosticReport)resource;
        super.addCommonResourceReferences(report, referenceValues);

        if (report.hasIdentifier()) {
            super.addIndentifierReferences(report.getIdentifier(), referenceValues);
        }
        if (report.hasSubject()) {
            super.addReference(report.getSubject(), referenceValues);
        }
        if (report.hasEncounter()) {
            super.addReference(report.getEncounter(), referenceValues);
        }
        if (report.hasPerformer()) {
            super.addReference(report.getPerformer(), referenceValues);
        }
        if (report.hasRequest()) {
            super.addReferences(report.getRequest(), referenceValues);
        }
        if (report.hasSpecimen()) {
            super.addReferences(report.getSpecimen(), referenceValues);
        }
        if (report.hasResult()) {
            super.addReferences(report.getResult(), referenceValues);
        }
        if (report.hasImagingStudy()) {
            super.addReferences(report.getImagingStudy(), referenceValues);
        }
        if (report.hasImage()) {
            for (DiagnosticReport.DiagnosticReportImageComponent image: report.getImage()) {
                if (image.hasLink()) {
                    super.addReference(image.getLink(), referenceValues);
                }
            }
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        DiagnosticReport report = (DiagnosticReport)resource;

        super.mapCommonResourceFields(report, mappings, failForMissingMappings);

        if (report.hasIdentifier()) {
            super.mapIdentifiers(report.getIdentifier(), mappings, failForMissingMappings);
        }
        if (report.hasSubject()) {
            super.mapReference(report.getSubject(), mappings, failForMissingMappings);
        }
        if (report.hasEncounter()) {
            super.mapReference(report.getEncounter(), mappings, failForMissingMappings);
        }
        if (report.hasPerformer()) {
            super.mapReference(report.getPerformer(), mappings, failForMissingMappings);
        }
        if (report.hasRequest()) {
            super.mapReferences(report.getRequest(), mappings, failForMissingMappings);
        }
        if (report.hasSpecimen()) {
            super.mapReferences(report.getSpecimen(), mappings, failForMissingMappings);
        }
        if (report.hasResult()) {
            super.mapReferences(report.getResult(), mappings, failForMissingMappings);
        }
        if (report.hasImagingStudy()) {
            super.mapReferences(report.getImagingStudy(), mappings, failForMissingMappings);
        }
        if (report.hasImage()) {
            for (DiagnosticReport.DiagnosticReportImageComponent image: report.getImage()) {
                if (image.hasLink()) {
                    super.mapReference(image.getLink(), mappings, failForMissingMappings);
                }
            }
        }

        
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        DiagnosticReport report = (DiagnosticReport)resource;
        if (report.hasSubject()) {
            return ReferenceHelper.getReferenceId(report.getSubject(), ResourceType.Patient);
        }
        return null;
    }

/*
    @Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        DiagnosticReport report = (DiagnosticReport)resource;

        if (report.hasIdentifier()) {
            super.mapIdentifiers(report.getIdentifier(), serviceId, systemId);
        }
        if (report.hasSubject()) {
            super.mapReference(report.getSubject(), serviceId, systemId);
        }
        if (report.hasEncounter()) {
            super.mapReference(report.getEncounter(), serviceId, systemId);
        }
        if (report.hasPerformer()) {
            super.mapReference(report.getPerformer(), serviceId, systemId);
        }
        if (report.hasRequest()) {
            super.mapReferences(report.getRequest(), serviceId, systemId);
        }
        if (report.hasSpecimen()) {
            super.mapReferences(report.getSpecimen(), serviceId, systemId);
        }
        if (report.hasResult()) {
            super.mapReferences(report.getResult(), serviceId, systemId);
        }
        if (report.hasImagingStudy()) {
            super.mapReferences(report.getImagingStudy(), serviceId, systemId);
        }
        if (report.hasImage()) {
            for (DiagnosticReport.DiagnosticReportImageComponent image: report.getImage()) {
                if (image.hasLink()) {
                    super.mapReference(image.getLink(), serviceId, systemId);
                }
            }
        }

        return super.mapCommonResourceFields(report, serviceId, systemId, mapResourceId);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
