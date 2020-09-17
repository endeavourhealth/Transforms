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
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.subscriber.targetTables.EncounterAdditional;
import org.endeavourhealth.transform.subscriber.targetTables.OutputContainer;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class EncounterTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(EncounterTransformer.class);

    private static final EncounterCodeDalI encounterCodeDal = DalProvider.factoryEncounterCodeDal();

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Encounter;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        //three tables, Encounter, EncounterEvent and EncounterAdditional may be written to in this transform
        org.endeavourhealth.transform.subscriber.targetTables.Encounter targetEncounterTable = params.getOutputContainer().getEncounters();
        org.endeavourhealth.transform.subscriber.targetTables.EncounterEvent targetEncounterEventTable = params.getOutputContainer().getEncounterEvents();
        org.endeavourhealth.transform.subscriber.targetTables.EncounterAdditional targetEncounterAdditionalTable = params.getOutputContainer().getEncounterAdditional();

        Encounter fhir = (Encounter)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {
            targetEncounterTable.writeDelete(subscriberId);
            targetEncounterEventTable.writeDelete(subscriberId);
            targetEncounterAdditionalTable.writeDelete(subscriberId);
            return;
        }

        long organizationId;
        long patientId;
        long personId;
        Long practitionerId = null;
        Long appointmentId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionConceptId = null;
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
        String admissionMethod = null;
        Date endDate = null;
        Long institutionLocationId = null;
        Date dateRecorded = null;

        organizationId = params.getSubscriberOrganisationId().longValue();
        patientId = params.getSubscriberPatientId().longValue();
        personId = params.getSubscriberPersonId().longValue();

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
                    practitionerId = transformOnDemandAndMapId(practitionerReference, SubscriberTableId.PRACTITIONER, params);
                }
            }
        }

        if (fhir.hasAppointment()) {
            Reference appointmentReference = fhir.getAppointment();
            appointmentId = transformOnDemandAndMapId(appointmentReference, SubscriberTableId.APPOINTMENT, params);
        }

        if (fhir.hasPeriod()) {
            Period period = fhir.getPeriod();
            DateTimeType dt = period.getStartElement();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionConceptId = convertDatePrecision(params, fhir, dt.getPrecision(), clinicalEffectiveDate.toString());
        }

        if (fhir.hasEpisodeOfCare()) {
            Reference episodeReference = fhir.getEpisodeOfCare().get(0);
            episodeOfCareId = transformOnDemandAndMapId(episodeReference, SubscriberTableId.EPISODE_OF_CARE, params);
        }

        if (fhir.hasServiceProvider()) {
            Reference orgReference = fhir.getServiceProvider();
            serviceProviderOrganisationId = transformOnDemandAndMapId(orgReference, SubscriberTableId.ORGANIZATION, params);
        }
        if (serviceProviderOrganisationId == null) {
            serviceProviderOrganisationId = params.getSubscriberOrganisationId();
        }

        String originalTerm = findEncounterTypeTerm(fhir, params);
        if (!Strings.isNullOrEmpty(originalTerm)) {

            originalTerm = originalTerm.toLowerCase();

            //change to new mapping for encounter types in the IM
            coreConceptId = IMHelper.getIMMappedConceptForTypeTerm(fhir, IMConstant.ENCOUNTER_LEGACY, originalTerm);
            nonCoreConceptId = IMHelper.getConceptDbidForTypeTerm(fhir, IMConstant.ENCOUNTER_LEGACY, originalTerm);
            /*coreConceptId = IMHelper.getIMMappedConceptForTypeTerm(fhir, IMConstant.DCE_type_of_encounter, originalTerm);
            nonCoreConceptId = IMHelper.getConceptDbidForTypeTerm(fhir, IMConstant.DCE_type_of_encounter, originalTerm);*/

            type = null;
            subtype = null;
        }


        if (fhir.getPatient() != null) {
            Reference ref = fhir.getPatient();
            Patient patient = params.getCachedPatient(ref);
            ageAtEvent = getPatientAgeInDecimalYears(patient, clinicalEffectiveDate);
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

            else {
                admissionMethod = encounterClass.toCode();
            }
        }


        if (fhir.hasPeriod()) {
            Period period = fhir.getPeriod();
            DateTimeType dt = period.getEndElement();
            endDate = dt.getValue();
        }

        institutionLocationId = transformLocationId(fhir, params);

        dateRecorded = params.includeDateRecorded(fhir);

        if (!fhir.hasPartOf()) {
            //if the FHIR Encounter is NOT part of another encounter, then write it to the regular encounter table

            //the location ID column is a String for some reason, so convert the long ID to a String
            String institutionLocationIdStr = null;
            if (institutionLocationId != null) {
                institutionLocationIdStr = "" + institutionLocationId;
            }

            targetEncounterTable.setIncludeDateRecorded(params.isIncludeDateRecorded());
            targetEncounterTable.writeUpsert(
                    subscriberId,
                    organizationId,
                    patientId,
                    personId,
                    practitionerId,
                    appointmentId,
                    clinicalEffectiveDate,
                    datePrecisionConceptId,
                    episodeOfCareId,
                    serviceProviderOrganisationId,
                      coreConceptId,
                     nonCoreConceptId,
                    ageAtEvent,
                    type,
                    subtype,
                    admissionMethod,
                    endDate,
                    institutionLocationIdStr,
                    dateRecorded);

            //we also need to populate the encounter_additional table with encounter extension data
            transformEncounterAdditionals(fhir, params, subscriberId);

        } else {
            //if the FHIR Encounter IS part of another encounter, then write it to the encounter event table
            //but ONLY if we know the target DB has the encounter event table
            Reference partOfReference = fhir.getPartOf();
            Long parentEncounterId = transformOnDemandAndMapId(partOfReference, SubscriberTableId.ENCOUNTER, params);

            //if the parent encounter has been deleted, don't transform this
            if (parentEncounterId == null) {
                return;
            }

            boolean isFinished = fhir.hasStatus() && fhir.getStatus() == Encounter.EncounterState.FINISHED;

            targetEncounterEventTable.writeUpsert(
                    subscriberId,
                    organizationId,
                    patientId,
                    personId,
                    parentEncounterId.longValue(),
                    practitionerId,
                    appointmentId,
                    clinicalEffectiveDate,
                    datePrecisionConceptId,
                    episodeOfCareId,
                    serviceProviderOrganisationId,
                    coreConceptId,
                    nonCoreConceptId,
                    ageAtEvent,
                    type,
                    subtype,
                    admissionMethod,
                    endDate,
                    institutionLocationId,
                    dateRecorded,
                    isFinished);

            //we also need to populate the encounter_additional table with encounter extension data
            transformEncounterAdditionals(fhir, params, subscriberId);
        }
    }

    private void transformEncounterAdditionals(Resource resource, SubscriberTransformHelper params, SubscriberId subscriberId) throws Exception {

        Encounter fhir = (Encounter)resource;

        //if it has no extension data, then nothing further to do
        if (!fhir.hasExtension()) {
            return;
        }

        //then for each additional extension parameter the additional data
        Extension additionalExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ADDITIONAL);

        if (additionalExtension != null) {

            Reference idReference = (Reference)additionalExtension.getValue();
            String idReferenceValue = idReference.getReference();
            idReferenceValue = idReferenceValue.substring(1); //remove the leading "#" char

            for (Resource containedResource: fhir.getContained()) {
                if (containedResource.getId().equals(idReferenceValue)) {

                    OutputContainer outputContainer = params.getOutputContainer();
                    EncounterAdditional encounterAdditional = outputContainer.getEncounterAdditional();

                    //additional extension data is stored as Parameter resources
                    Parameters parameters = (Parameters)containedResource;

                    //get all the entries in the parameters list
                    List<Parameters.ParametersParameterComponent> entries = parameters.getParameter();
                    for (Parameters.ParametersParameterComponent parameter : entries) {

                        //each parameter entry will have a key value pair of either:
                        //  1) JSON_name plus JSON data as value
                        //  2) IM concept plus a CodeableConcept value
                        if (parameter.hasName() && parameter.hasValue()) {

                            String propertyCode = parameter.getName();
                            if (!propertyCode.startsWith("JSON_")) {

                                //these values are from IM API mapping
                                String propertyScheme = IMConstant.DISCOVERY_CODE;
                                String type = parameter.getValue().getClass().getSimpleName();
                                if (type.equalsIgnoreCase("CodeableConcept")) {
                                    CodeableConcept parameterValue = (CodeableConcept) parameter.getValue();
                                    String valueCode = parameterValue.getCoding().get(0).getCode();
                                    String valueScheme = parameterValue.getCoding().get(0).getSystem();
                                    //we need to look up DBids for both
                                    Integer propertyConceptDbid =
                                            IMClient.getConceptDbidForSchemeCode(propertyScheme, propertyCode);
                                    Integer valueConceptDbid =
                                            IMClient.getConceptDbidForSchemeCode(valueScheme, valueCode);

                                    //transform the IM values to the encounter_additional table upsert
                                    encounterAdditional.writeUpsert(subscriberId, propertyConceptDbid, valueConceptDbid);
                                } else {
                                    //TODO handle String values
                                }
                            } else {
                                //TODO: Handle extended additional Json here
                            }
                        }
                    }
                    break;
                }
            }
        }
    }

    private static Long transformLocationId(Encounter encounter, SubscriberTransformHelper params) throws Exception {

        if (!encounter.hasLocation()) {
            return null;
        }

        for (Encounter.EncounterLocationComponent loc: encounter.getLocation()) {
            if (loc.getStatus() == Encounter.EncounterLocationStatus.ACTIVE) {
                Reference ref = loc.getLocation();
                return transformOnDemandAndMapId(ref, SubscriberTableId.LOCATION, params);
            }
        }

        return null;
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

    /*private String findAdtMessageCode(Encounter fhir) {
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
    }*/

    private Date findEndDate(Encounter fhir) {
        if (fhir.hasPeriod()) {
            Period p = fhir.getPeriod();
            if (p.hasEnd()) {
                return p.getEnd();
            }
        }

        return null;
    }

    private Long findLocationId(Encounter fhir, SubscriberTransformHelper params) throws Exception {

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
            return transformOnDemandAndMapId(locationReference, SubscriberTableId.LOCATION, params);
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
                                       long episodeOfCareId, Date clinicalEffectiveDate, Integer datePrecisionConceptId, Long appointmentId,
                                       Long serviceProviderOrganisationId, Long recordingPractitionerId, Date recordingDate,
                                       Long locationId, Date endDate, Integer duration, Integer durationUnit) throws Exception {



        OutputContainer outputContainer = params.getOutputContainer();



    }*/

    /*public static String findEncounterTypeTerm(Encounter fhir) {
        return findEncounterTypeTerm(fhir, null);
    }*/

    private static String findEncounterTypeTerm(Encounter fhir, SubscriberTransformHelper params) {

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
