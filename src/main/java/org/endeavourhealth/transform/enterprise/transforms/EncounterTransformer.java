package org.endeavourhealth.transform.enterprise.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.rdbms.reference.EncounterCode;
import org.endeavourhealth.core.rdbms.reference.EncounterCodeHelper;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformParams;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class EncounterTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(EncounterTransformer.class);


    public boolean shouldAlwaysTransform() {
        return true;
    }

    public void transform(Long enterpriseId,
                          Resource resource,
                          AbstractEnterpriseCsvWriter csvWriter,
                          EnterpriseTransformParams params) throws Exception {

        Encounter fhir = (Encounter)resource;

        long id;
        long organisationId;
        long patientId;
        long personId;
        Long practitionerId = null;
        Long appointmentId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionId = null;
        Long snomedConceptId = null;
        String originalCode = null;
        String originalTerm = null;
        Long episodeOfCareId = null;
        Long serviceProviderOrganisationId = null;

        id = enterpriseId.longValue();
        organisationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasParticipant()) {

            for (Encounter.EncounterParticipantComponent participantComponent: fhir.getParticipant()) {

                boolean primary = false;
                for (CodeableConcept codeableConcept: participantComponent.getType()) {
                    for (Coding coding : codeableConcept.getCoding()) {
                        if (coding.getCode().equals(EncounterParticipantType.PRIMARY_PERFORMER.getCode())) {
                            primary = true;
                            break;
                        }
                    }
                }

                if (primary) {
                    Reference practitionerReference = participantComponent.getIndividual();
                    practitionerId = findEnterpriseId(params, practitionerReference);
                    if (practitionerId == null) {
                        practitionerId = transformOnDemand(practitionerReference, params);
                    }
                }
            }
        }

        if (fhir.hasAppointment()) {
            Reference appointmentReference = fhir.getAppointment();
            appointmentId = findEnterpriseId(params, appointmentReference);
        }

        if (fhir.hasPeriod()) {
            Period period = fhir.getPeriod();
            DateTimeType dt = period.getStartElement();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        if (fhir.hasExtension()) {

            Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ENCOUNTER_SOURCE);
            if (extension != null) {
                CodeableConcept codeableConcept = (CodeableConcept)extension.getValue();

                snomedConceptId = CodeableConceptHelper.findSnomedConceptId(codeableConcept);

                //add the raw original code and term, to assist in data checking and results display
                originalCode = CodeableConceptHelper.findOriginalCode(codeableConcept);
                originalTerm = codeableConcept.getText();

            } else {
                //if we don't have the source extension giving the snomed code, then see if this is a secondary care encounter
                //and see if we can generate a snomed ID from the encounter fields
                EncounterCode code = mapEncounterCode(fhir);
                if (code != null) {
                    snomedConceptId = code.getCode();
                    originalTerm = code.getTerm();
                }
            }
        }

        if (fhir.hasEpisodeOfCare()) {
            if (fhir.getEpisodeOfCare().size() > 1) {
                throw new TransformException("Can't handle encounters linked to more than one episode of care");
            }
            Reference episodeReference = fhir.getEpisodeOfCare().get(0);
            episodeOfCareId = findEnterpriseId(params, episodeReference);
        }

        if (fhir.hasServiceProvider()) {
            Reference orgReference = fhir.getServiceProvider();
            serviceProviderOrganisationId = findEnterpriseId(params, orgReference);
        }
        if (serviceProviderOrganisationId == null) {
            serviceProviderOrganisationId = params.getEnterpriseOrganisationId();
        }

        org.endeavourhealth.transform.enterprise.outputModels.Encounter model = (org.endeavourhealth.transform.enterprise.outputModels.Encounter)csvWriter;
        model.writeUpsert(id,
            organisationId,
            patientId,
            personId,
            practitionerId,
            appointmentId,
            clinicalEffectiveDate,
            datePrecisionId,
            snomedConceptId,
            originalCode,
            originalTerm,
            episodeOfCareId,
            serviceProviderOrganisationId);
    }

    private EncounterCode mapEncounterCode(Encounter fhir) throws Exception {

        Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.HL7_MESSAGE_TYPE);

        //if the fhir resource doesn't have any of the secondary care fields we need, then return out
        if (extension == null
                || !fhir.hasType()
                || !fhir.hasClass_()) {
            return null;
        }

        CodeableConcept codeableConcept = (CodeableConcept)extension.getValue();
        Coding hl7MessageTypeCoding = CodeableConceptHelper.findCoding(codeableConcept, FhirUri.CODE_SYSTEM_HL7V2_MESSAGE_TYPE);
        if (hl7MessageTypeCoding == null) {
            LOG.debug("No HL7 type coding found in " + fhir.getResourceType() + " " + fhir.getId());
            return null;
        }
        String hl7MessageTypeCode = hl7MessageTypeCoding.getCode();
        String hl7MessageTypeText = hl7MessageTypeCoding.getDisplay();

        String clsDesc = null;

        //if our class is "other" and there's an extension, then get the class out of there
        Encounter.EncounterClass cls = fhir.getClass_();
        if (cls == Encounter.EncounterClass.OTHER
                && fhir.hasClass_Element()
                && fhir.getClass_Element().hasExtension()) {

            for (Extension classExtension: fhir.getClass_Element().getExtension()) {
                if (classExtension.getUrl().equals(FhirExtensionUri.ENCOUNTER_CLASS)) {
                    //not 100% of the type of the value, so just append to a String
                    clsDesc = "" + classExtension.getValue();
                }
            }
        }

        //if it wasn't "other" or didn't have an extension
        if (Strings.isNullOrEmpty(clsDesc)) {
            clsDesc = cls.toCode();
        }

        String typeCode = null;
        String typeDesc = null;
        for (CodeableConcept typeCodeableConcept: fhir.getType()) {

            //there should only be a single codeable concept, so just assign this
            typeDesc = typeCodeableConcept.getText();

            for (Coding coding: typeCodeableConcept.getCoding()) {
                if (coding.hasSystem()
                        && (coding.getSystem().equals(FhirValueSetUri.VALUE_SET_ENCOUNTER_TYPE_BARTS)
                        || coding.getSystem().equals(FhirValueSetUri.VALUE_SET_ENCOUNTER_TYPE_HOMERTON))) {
                    typeCode = coding.getCode();
                }
            }
        }

        //seems a fairly solid pattern to combine these to create something meaningful
        String term = typeDesc + " " + hl7MessageTypeText;

        EncounterCode ret = EncounterCodeHelper.findOrCreateCode(term, hl7MessageTypeCode, clsDesc, typeCode);
        if (ret == null) {
            LOG.debug("Null ret for term " + term + " message type " + hl7MessageTypeCode + " cls " + clsDesc + " type " + typeCode + " in " + fhir.getResourceType() + " " + fhir.getId());
        } else {
            LOG.debug("ret " + ret.getCode() + " for term " + term + " message type " + hl7MessageTypeCode + " cls " + clsDesc + " type " + typeCode + " in " + fhir.getResourceType() + " " + fhir.getId());
        }
        return ret;
    }

}
