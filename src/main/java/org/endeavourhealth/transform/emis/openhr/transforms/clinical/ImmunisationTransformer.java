package org.endeavourhealth.transform.emis.openhr.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.emis.openhr.schema.OpenHR001HealthDomain;
import org.endeavourhealth.transform.emis.openhr.transforms.common.CodeConverter;
import org.endeavourhealth.transform.emis.openhr.transforms.common.DateConverter;
import org.endeavourhealth.transform.emis.openhr.transforms.common.EventEncounterMap;
import org.hl7.fhir.instance.model.Immunization;
import org.hl7.fhir.instance.model.MedicationAdministration;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.ResourceType;

public class ImmunisationTransformer implements ClinicalResourceTransformer
{
    public Immunization transform(OpenHR001HealthDomain.Event source, OpenHR001HealthDomain healthDomain, EventEncounterMap eventEncounterMap) throws TransformException
    {
        Immunization target = new Immunization();
        target.setId(source.getId());
        target.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_IMMUNIZATION));

        target.setStatus(MedicationAdministration.MedicationAdministrationStatus.COMPLETED.toCode());
        target.setDateElement(DateConverter.convertPartialDateTimeToDateTimeType(source.getEffectiveTime()));
        target.setPatient(ReferenceHelper.createReference(ResourceType.Patient, source.getPatient()));
        target.setPerformer(ReferenceHelper.createReference(ResourceType.Practitioner, source.getAuthorisingUserInRole()));
        target.setEncounter(eventEncounterMap.getEncounterReference(source.getId()));
        target.setVaccineCode(CodeConverter.convertCode(source.getCode(), source.getDisplayTerm()));
        return target;
    }
}