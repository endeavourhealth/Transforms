package org.endeavourhealth.transform.enterprise.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.reference.EncounterCodeDalI;
import org.endeavourhealth.core.database.dal.reference.models.EncounterCode;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformParams;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class EncounterTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(EncounterTransformer.class);

    private static final EncounterCodeDalI encounterCodeDal = DalProvider.factoryEncounterCodeDal();

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
                    practitionerId = transformOnDemandAndMapId(practitionerReference, params);
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

        //changing to use our information model to get the concept ID for the consultation type based on the textual term
        originalTerm = findEncounterTypeTerm(fhir, params);
        if (!Strings.isNullOrEmpty(originalTerm)) {
            EncounterCode ret = encounterCodeDal.findOrCreateCode(originalTerm);
            snomedConceptId = ret.getCode();
        }

        /*if (fhir.hasExtension()) {

            Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ENCOUNTER_SOURCE);
            if (extension != null) {
                CodeableConcept codeableConcept = (CodeableConcept) extension.getValue();

                snomedConceptId = CodeableConceptHelper.findSnomedConceptId(codeableConcept);

                //add the raw original code and term, to assist in data checking and results display
                originalCode = CodeableConceptHelper.findOriginalCode(codeableConcept);
                originalTerm = codeableConcept.getText();
            }
        }

        //if we don't have the source extension giving the snomed code, then see if this is a secondary care encounter
        //and see if we can generate a snomed ID from the encounter fields
        if (Strings.isNullOrEmpty(originalCode)) {

            EncounterCode code = mapEncounterCode(fhir, params);
            if (code != null) {
                snomedConceptId = code.getCode();
                originalTerm = code.getTerm();
            }
        }*/

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

    private String findEncounterTypeTerm(Encounter fhir, EnterpriseTransformParams params) {

        if (fhir.hasExtension()) {
            Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ENCOUNTER_SOURCE);
            if (extension != null) {
                CodeableConcept codeableConcept = (CodeableConcept) extension.getValue();
                String term = codeableConcept.getText();
                if (!Strings.isNullOrEmpty(term)) {
                    return term;
                }
            }
        }

        //if the fhir resource doesn't have a type or class, then return out
        if (!fhir.hasType()
                || !fhir.hasClass_()) {
            //LOG.debug("No type or class");
            return null;
        }

        String hl7MessageTypeText = null;

        //newer versions of the Emcounters have an extension that gives the ADT message type,
        //but for older ones we'll need to look back at the original exchange t0 find the type
        Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.HL7_MESSAGE_TYPE);

        if (extension != null) {
            CodeableConcept codeableConcept = (CodeableConcept) extension.getValue();
            Coding hl7MessageTypeCoding = CodeableConceptHelper.findCoding(codeableConcept, FhirUri.CODE_SYSTEM_HL7V2_MESSAGE_TYPE);
            if (hl7MessageTypeCoding == null) {
                //LOG.debug("No HL7 type coding found in " + fhir.getResourceType() + " " + fhir.getId());
                return null;
            }
            hl7MessageTypeText = hl7MessageTypeCoding.getDisplay();
            //LOG.debug("Got hl7 type " + hl7MessageTypeText + " from extension");

        } else {
            try {
                String exchangeBody = params.getExchangeBody();
                Bundle bundle = (Bundle)FhirResourceHelper.deserialiseResouce(exchangeBody);
                for (Bundle.BundleEntryComponent entry: bundle.getEntry()) {
                    if (entry.getResource() != null
                            && entry.getResource() instanceof MessageHeader) {

                        MessageHeader header = (MessageHeader)entry.getResource();
                        if (header.hasEvent()) {
                            Coding coding = header.getEvent();
                            hl7MessageTypeText = coding.getDisplay();

                            //LOG.debug("Got hl7 type " + hl7MessageTypeText + " from exchange body");
                        }
                    }
                }
            } catch (Exception ex) {
                //if the exchange body isn't a FHIR bundle, then we'll get an error by treating as such, so just ignore them
            }
        }

        //if we couldn't find an HL7 message type, then give up
        if (Strings.isNullOrEmpty(hl7MessageTypeText)) {
            //LOG.debug("Failed to find hl7 type");
            return null;
        }

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

        String typeDesc = null;
        for (CodeableConcept typeCodeableConcept: fhir.getType()) {
            //there should only be a single codeable concept, so just assign this
            typeDesc = typeCodeableConcept.getText();
        }

        //seems a fairly solid pattern to combine these to create something meaningful
        String term = typeDesc + " " + hl7MessageTypeText;
        if (!clsDesc.equalsIgnoreCase(typeDesc)) {
            term += " (" + clsDesc + ")";
        }

        return term;
    }


    /*private EncounterCode mapEncounterCode(Encounter fhir, EnterpriseTransformParams params) throws Exception {

        //if the fhir resource doesn't have a type or class, then return out
        if (!fhir.hasType()
            || !fhir.hasClass_()) {
            LOG.debug("No type or class");
            return null;
        }

        String hl7MessageTypeCode = null;
        String hl7MessageTypeText = null;

        //newer versions of the Emcounters have an extension that gives the ADT message type,
        //but for older ones we'll need to look back at the original exchange t0 find the type
        Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.HL7_MESSAGE_TYPE);

        if (extension != null) {
            CodeableConcept codeableConcept = (CodeableConcept) extension.getValue();
            Coding hl7MessageTypeCoding = CodeableConceptHelper.findCoding(codeableConcept, FhirUri.CODE_SYSTEM_HL7V2_MESSAGE_TYPE);
            if (hl7MessageTypeCoding == null) {
                LOG.debug("No HL7 type coding found in " + fhir.getResourceType() + " " + fhir.getId());
                return null;
            }
            hl7MessageTypeCode = hl7MessageTypeCoding.getCode();
            hl7MessageTypeText = hl7MessageTypeCoding.getDisplay();
            LOG.debug("Got hl7 type " + hl7MessageTypeCode + " from extension");

        } else {
            try {
                String exchangeBody = params.getExchangeBody();
                Bundle bundle = (Bundle)FhirResourceHelper.deserialiseResouce(exchangeBody);
                for (Bundle.BundleEntryComponent entry: bundle.getEntry()) {
                    if (entry.getResource() != null
                            && entry.getResource() instanceof MessageHeader) {

                        MessageHeader header = (MessageHeader)entry.getResource();
                        if (header.hasEvent()) {
                            Coding coding = header.getEvent();
                            hl7MessageTypeCode = coding.getCode();
                            hl7MessageTypeText = coding.getDisplay();

                            LOG.debug("Got hl7 type " + hl7MessageTypeCode + " from exchange body");
                        }
                    }
                }
            } catch (Exception ex) {
                //if the exchange body isn't a FHIR bundle, then we'll get an error by treating as such, so just ignore them
            }
        }

        //if we couldn't find an HL7 message type, then give up
        if (Strings.isNullOrEmpty(hl7MessageTypeCode)) {
            LOG.debug("Failed to find hl7 type");
            return null;
        }

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
        if (!clsDesc.equalsIgnoreCase(typeDesc)) {
            term += " (" + clsDesc + ")";
        }

        EncounterCode ret = EncounterCodeHelper.findOrCreateCode(term, hl7MessageTypeCode, clsDesc, typeCode);
        if (ret == null) {
            LOG.debug("Null ret for term " + term + " message type " + hl7MessageTypeCode + " cls " + clsDesc + " type " + typeCode + " in " + fhir.getResourceType() + " " + fhir.getId());
        } else {
            LOG.debug("ret " + ret.getCode() + " for term " + term + " message type " + hl7MessageTypeCode + " cls " + clsDesc + " type " + typeCode + " in " + fhir.getResourceType() + " " + fhir.getId());
        }
        return ret;
    }*/

}
