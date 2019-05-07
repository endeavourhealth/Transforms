package org.endeavourhealth.transform.subscriber.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.reference.EncounterCodeDalI;
import org.endeavourhealth.core.database.dal.reference.models.EncounterCode;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class EncounterTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(EncounterTransformer.class);

    private static final EncounterCodeDalI encounterCodeDal = DalProvider.factoryEncounterCodeDal();

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Encounter model = params.getOutputContainer().getEncounters();

        if (resourceWrapper.isDeleted()) {
            model.writeDelete(subscriberId);

            return;
        }

        Encounter fhir = (Encounter) FhirResourceHelper.deserialiseResouce(resourceWrapper);

        long organizationId;
        long patientId;
        long personId;
        Long practitionerId = null;
        Long appointmentId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionId = null;
        // Long snomedConceptId = null;
        // String originalCode = null;
        // String originalTerm = null;
        Long episodeOfCareId = null;
        Long serviceProviderOrganisationId = null;
        Integer coreConceptId = null;
        Integer nonCoreConceptId = null;
        Double ageAtEvent = null;
        String type = null;
        String subtype = null;
        Boolean isPrimary = null;
        String admissionMethod = null;
        Date endDate = null;
        String institutionLocationId = null;

        organizationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasParticipant()) {

            for (Encounter.EncounterParticipantComponent participantComponent: fhir.getParticipant()) {

                boolean primary = false;
                for (CodeableConcept codeableConcept: participantComponent.getType()) {
                    for (Coding coding : codeableConcept.getCoding()) {
                        String typeCode = coding.getCode();
                        if (typeCode.equals(EncounterParticipantType.PRIMARY_PERFORMER.getCode()) //used for GP
                                || typeCode.equals(EncounterParticipantType.ATTENDER.getCode())) { //used for ADT
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
            appointmentId = findEnterpriseId(params, SubscriberTableId.APPOINTMENT, appointmentReference);
        }

        if (fhir.hasPeriod()) {
            Period period = fhir.getPeriod();
            DateTimeType dt = period.getStartElement();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        /*
        //changing to use our information model to get the concept ID for the consultation type based on the textual term
        originalTerm = findEncounterTypeTerm(fhir, params);
        if (!Strings.isNullOrEmpty(originalTerm)) {
            EncounterCode ret = encounterCodeDal.findOrCreateCode(originalTerm);
            snomedConceptId = ret.getCode();
        }*/

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
            Reference episodeReference = fhir.getEpisodeOfCare().get(0);
            episodeOfCareId = findEnterpriseId(params, SubscriberTableId.EPISODE_OF_CARE, episodeReference);
        }

        if (fhir.hasServiceProvider()) {
            Reference orgReference = fhir.getServiceProvider();
            serviceProviderOrganisationId = findEnterpriseId(params, SubscriberTableId.ORGANIZATION, orgReference);
        }
        if (serviceProviderOrganisationId == null) {
            serviceProviderOrganisationId = params.getEnterpriseOrganisationId();
        }

        // TODO Code needs to be reviewed to use the IM for
        //  Core Concept Id and Non Core Concept ID
        String originalCode = null;
        String originalTerm = null;

        originalTerm = findEncounterTypeTerm(fhir, params);
        if (!Strings.isNullOrEmpty(originalTerm)) {
            encounterCodeDal.findOrCreateCode(originalTerm);
            //snomedConceptId = ret.getCode();

            originalTerm = originalTerm.toLowerCase();
            coreConceptId = IMClient.getMappedCoreConceptIdForTypeTerm(IMConstant.DCE_Type_of_encounter, originalTerm);
            if (coreConceptId == null) {
                LOG.warn("coreConceptId is null using type: " + IMConstant.DCE_Type_of_encounter + " term: " + originalTerm);
                throw new TransformException("coreConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
            }

            nonCoreConceptId = IMClient.getConceptIdForTypeTerm(IMConstant.DCE_Type_of_encounter, originalTerm);
            if (nonCoreConceptId == null) {
                LOG.warn("nonCoreConceptId is null using type: " + IMConstant.DCE_Type_of_encounter + " term: " + originalTerm);
                throw new TransformException("nonCoreConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
            }
        }

        if (fhir.hasExtension()) {
            Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ENCOUNTER_SOURCE);
            if (extension != null) {
                CodeableConcept codeableConcept = (CodeableConcept) extension.getValue();

                originalTerm = codeableConcept.getText().toLowerCase();
                coreConceptId = IMClient.getMappedCoreConceptIdForTypeTerm(IMConstant.DCE_Type_of_encounter, originalTerm);
                if (coreConceptId == null) {
                    LOG.warn("coreConceptId is null using type: " + IMConstant.DCE_Type_of_encounter + " term: " + originalTerm);
                    throw new TransformException("coreConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
                }

                nonCoreConceptId = IMClient.getConceptIdForTypeTerm(IMConstant.DCE_Type_of_encounter, originalTerm);
                if (nonCoreConceptId == null) {
                    LOG.warn("nonCoreConceptId is null using type: " + IMConstant.DCE_Type_of_encounter + " term: " + originalTerm);
                    throw new TransformException("nonCoreConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
                }
            }
        }

        if (fhir.getPatientTarget() != null) {
            ageAtEvent = getPatientAgeInMonths(fhir.getPatientTarget());
        }

        // TODO Code needs to be added to use the IM for
        //  Encounter Type
        type = null;

        // TODO Code needs to be added to use the IM for
        //  Encounter Subtype
        subtype = null;

        Extension isPrimaryExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_PRIMARY);
        if (isPrimaryExtension != null) {
            BooleanType b = (BooleanType)isPrimaryExtension.getValue();
            if (b.getValue() != null) {
                isPrimary = b.getValue();
            }
        }

        if (fhir.hasClass_()) {

            Encounter.EncounterClass encounterClass = fhir.getClass_();

            if (encounterClass == Encounter.EncounterClass.OTHER
                    && fhir.hasClass_Element()
                    && fhir.getClass_Element().hasExtension()) {

                for (Extension classExtension: fhir.getClass_Element().getExtension()) {

                    if (classExtension.getUrl().equals(FhirExtensionUri.ENCOUNTER_CLASS)) {
                        admissionMethod = "" + classExtension.getValue();
                    }
                }
            }

            else {admissionMethod = encounterClass.toCode();}
        }


        if (fhir.hasPeriod()) {
            Period period = fhir.getPeriod();
            DateTimeType dt = period.getEndElement();
            endDate = dt.getValue();
        }

        Extension locationRefExt = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ENCOUNTER_LOCATION_REFERENCE);
        if (locationRefExt != null) {
            StringType ref = (StringType) locationRefExt.getValue();
            if (ref.getValue() != null) {
                institutionLocationId = ref.getValue();
            }
        }


        model.writeUpsert(subscriberId,
            organizationId,
            patientId,
            personId,
            practitionerId,
            appointmentId,
            clinicalEffectiveDate,
            datePrecisionId,
            episodeOfCareId,
            serviceProviderOrganisationId,
            coreConceptId,
            nonCoreConceptId,
            ageAtEvent,
            type,
            subtype,
            isPrimary,
            admissionMethod,
            endDate,
            institutionLocationId);

        //we also need to populate the two new encounter tables
        //tranformExtraEncounterTables(resource, params,
        //        id, organizationId, patientId, personId, practitionerId,
        //        episodeOfCareId, clinicalEffectiveDate, datePrecisionId, appointmentId,
        //        serviceProviderOrganisationId);
    }


    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.ENCOUNTER;
    }


    /*private Integer findDuration(Date startDate, Date endDate) {
        if (startDate == null
                || endDate == null) {
            return null;
        }

        long msDiff = endDate.getTime() - startDate.getTime();
        long secDiff = msDiff / 1000;
        long minDur = secDiff / 60;
        return new Integer((int)minDur);
    }*/


    private static CodeableConcept findSourceCodeableConcept(Encounter fhir) {
        if (!fhir.hasExtension()) {
            return null;
        }

        Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ENCOUNTER_SOURCE);
        if (extension == null) {
            return null;
        }

        return (CodeableConcept)extension.getValue();
    }


    /*private String findOriginalTerm(Encounter fhir) {
        if (!fhir.hasExtension()) {
            return null;
        }

        Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ENCOUNTER_SOURCE);
        if (extension == null) {
            return null;
        }

        CodeableConcept codeableConcept = (CodeableConcept)extension.getValue();
        return codeableConcept.getText();
    }

    private String findOriginalReadCode(Encounter fhir) throws Exception {
        if (!fhir.hasExtension()) {
            return null;
        }

        Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ENCOUNTER_SOURCE);
        if (extension == null) {
            return null;
        }

        CodeableConcept codeableConcept = (CodeableConcept)extension.getValue();
        return ObservationTransformer.findAndFormatOriginalCode(codeableConcept);
        //return CodeableConceptHelper.findOriginalCode(codeableConcept);
    }

    private Long findSnomedSourceConceptId(Encounter fhir) {
        if (!fhir.hasExtension()) {
            return null;
        }

        Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ENCOUNTER_SOURCE);
        if (extension == null) {
            return null;
        }

        CodeableConcept codeableConcept = (CodeableConcept)extension.getValue();
        return CodeableConceptHelper.findSnomedConceptId(codeableConcept);
    }*/

    private String findStatus(Encounter fhir) {
        if (!fhir.hasStatus()) {
            return null;
        }

        Encounter.EncounterState encounterState = fhir.getStatus();
        return encounterState.toCode();
    }

    private String findType(Encounter fhir) {
        if (!fhir.hasType()) {
            return null;
        }

        //only seem to ever have one type
        CodeableConcept codeableConcept = fhir.getType().get(0);
        return codeableConcept.getText();
    }

    private String findClass(Encounter fhir) {
        if (!fhir.hasClass_()) {
            return null;
        }

        Encounter.EncounterClass encounterClass = fhir.getClass_();
        if (encounterClass == Encounter.EncounterClass.OTHER
                && fhir.hasClass_Element()
                && fhir.getClass_Element().hasExtension()) {

            for (Extension classExtension: fhir.getClass_Element().getExtension()) {
                if (classExtension.getUrl().equals(FhirExtensionUri.ENCOUNTER_CLASS)) {
                    return "" + classExtension.getValue();
                }
            }
        }

        return encounterClass.toCode();
    }

    private String findAdtMessageCode(Encounter fhir) {
        if (!fhir.hasExtension()) {
            return null;
        }

        Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.HL7_MESSAGE_TYPE);
        if (extension == null) {
            return null;
        }

        CodeableConcept codeableConcept = (CodeableConcept) extension.getValue();
        Coding hl7MessageTypeCoding = CodeableConceptHelper.findCoding(codeableConcept, FhirCodeUri.CODE_SYSTEM_HL7V2_MESSAGE_TYPE);
        if (hl7MessageTypeCoding == null) {
            return null;
        }

        return hl7MessageTypeCoding.getCode();
    }

    private Long findRecordingPractitionerId(Encounter fhir, SubscriberTransformParams params) throws Exception {
        if (!fhir.hasExtension()) {
            return null;
        }

        Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_BY);
        if (extension == null) {
            return null;
        }

        Reference reference = (Reference)extension.getValue();
        return transformOnDemandAndMapId(reference, params);
    }

    private Date findEndDate(Encounter fhir) {
        if (fhir.hasPeriod()) {
            Period p = fhir.getPeriod();
            if (p.hasEnd()) {
                return p.getEnd();
            }
        }

        return null;
    }

    private Long findLocationId(Encounter fhir, SubscriberTransformParams params) throws Exception {

        if (!fhir.hasLocation()) {
            return null;
        }

        Reference locationReference = null;

        //use an active location
        for (Encounter.EncounterLocationComponent location: fhir.getLocation()) {
            if (location.hasStatus()
                    && location.getStatus() == Encounter.EncounterLocationStatus.ACTIVE) {

                locationReference = location.getLocation();
            }
        }

        //if no active location, use a completed one
        if (locationReference == null) {
            for (Encounter.EncounterLocationComponent location: fhir.getLocation()) {
                if (location.hasStatus()
                        && location.getStatus() == Encounter.EncounterLocationStatus.COMPLETED) {

                    locationReference = location.getLocation();
                }
            }
        }

        //if no completed or active location any
        if (locationReference == null) {
            for (Encounter.EncounterLocationComponent location: fhir.getLocation()) {
                locationReference = location.getLocation();
            }
        }

        if (locationReference == null) {
            return null;

        } else {
            return transformOnDemandAndMapId(locationReference, params);
        }
    }

    private Date findRecordingDate(Encounter fhir) {
        if (fhir.hasExtension()) {
            Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_DATE);
            if (extension != null) {
                DateTimeType dtt = (DateTimeType)extension.getValue();
                return dtt.getValue();
            }
        }

        return null;
    }

    /*private void transformEncounterRaw(Long enterpriseId, Resource resource, SubscriberTransformParams params,
                                       long id, long organisationId, long patientId, long personId, long practitionerId,
                                       long episodeOfCareId, Date clinicalEffectiveDate, Integer datePrecisionId, Long appointmentId,
                                       Long serviceProviderOrganisationId, Long recordingPractitionerId, Date recordingDate,
                                       Long locationId, Date endDate, Integer duration, Integer durationUnit) throws Exception {



        OutputContainer outputContainer = params.getOutputContainer();



    }*/

    /*public static String findEncounterTypeTerm(Encounter fhir) {
        return findEncounterTypeTerm(fhir, null);
    }*/

    private static String findEncounterTypeTerm(Encounter fhir, SubscriberTransformParams params) {

        String source = null;

        CodeableConcept sourceCodeableConcept = findSourceCodeableConcept(fhir);
        if (sourceCodeableConcept != null) {
            source = sourceCodeableConcept.getText();
        }

        //if the fhir resource doesn't have a type or class, then return out
        if (!fhir.hasType()
                || !fhir.hasClass_()) {
            //LOG.debug("No type or class");
            return source;
        }

        String hl7MessageTypeText = null;

        //newer versions of the Emcounters have an extension that gives the ADT message type,
        //but for older ones we'll need to look back at the original exchange t0 find the type
        Extension extension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.HL7_MESSAGE_TYPE);

        if (extension != null) {
            CodeableConcept codeableConcept = (CodeableConcept) extension.getValue();
            Coding hl7MessageTypeCoding = CodeableConceptHelper.findCoding(codeableConcept, FhirCodeUri.CODE_SYSTEM_HL7V2_MESSAGE_TYPE);
            if (hl7MessageTypeCoding == null) {
                //LOG.debug("No HL7 type coding found in " + fhir.getResourceType() + " " + fhir.getId());
                return null;
            }
            hl7MessageTypeText = hl7MessageTypeCoding.getDisplay();
            //LOG.debug("Got hl7 type " + hl7MessageTypeText + " from extension");
        }
        //all these older ones are well in the past, so remove this now so we don't end up retrieving the Exchange
        //for every Encounter resource
        /*else if (params != null) {
            //for older formats of the transformed resources, the HL7 message type can only be found from the raw original exchange body
            try {
                String exchangeBody = params.getExchangeBody();
                //only try to treat as JSON if it starts with a brace
                if (exchangeBody.trim().startsWith("{")) {
                    Bundle bundle = (Bundle) FhirResourceHelper.deserialiseResouce(exchangeBody);
                    for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
                        if (entry.getResource() != null
                                && entry.getResource() instanceof MessageHeader) {

                            MessageHeader header = (MessageHeader) entry.getResource();
                            if (header.hasEvent()) {
                                Coding coding = header.getEvent();
                                hl7MessageTypeText = coding.getDisplay();

                                //LOG.debug("Got hl7 type " + hl7MessageTypeText + " from exchange body");
                            }
                        }
                    }
                }
            } catch (Exception ex) {
                //if the exchange body isn't a FHIR bundle, then we'll get an error by treating as such, so just ignore them
            }
        }*/

        //if we couldn't find an HL7 message type, then give up
        //Barts Data Warehouse encounters don't have an HL7 message type, so removed this now
        /*if (Strings.isNullOrEmpty(hl7MessageTypeText)) {
            //LOG.debug("Failed to find hl7 type");
            return null;
        }*/

        //get class
        Encounter.EncounterClass cls = fhir.getClass_();
        String clsDesc = cls.toCode();

        //if our class is "other" and there's an extension, then get the class out of there
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

        //get type
        String typeDesc = null;
        for (CodeableConcept typeCodeableConcept: fhir.getType()) {
            //there should only be a single codeable concept, so just assign this
            typeDesc = typeCodeableConcept.getText();
        }

        //parameters:
        //source
        //type
        //cls
        //HL7 message type

        //seems a fairly solid pattern to combine these to create something meaningful
        String term = "";

        if (!Strings.isNullOrEmpty(typeDesc)) {
            term += " ";
            term += typeDesc;
        }

        if (!Strings.isNullOrEmpty(source)) {
            term += " ";
            term += source;
        }

        if (!Strings.isNullOrEmpty(hl7MessageTypeText)) {
            term += " ";
            term += hl7MessageTypeText;
        }

        if (!clsDesc.equalsIgnoreCase(typeDesc)) {
            term += " (" + clsDesc + ")";
        }

        return term.trim();
    }


    /*private EncounterCode mapEncounterCode(Encounter fhir, SubscriberTransformParams params) throws Exception {

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
            Coding hl7MessageTypeCoding = CodeableConceptHelper.findCoding(codeableConcept, FhirCodeUri.CODE_SYSTEM_HL7V2_MESSAGE_TYPE);
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
