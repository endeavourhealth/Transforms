package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.AppointmentAdditional;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class AppointmentTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AppointmentTransformer.class);
    public static final String PROP_CODE_APPOINTMENT_TYPE = "DM_AppointmentType";
    public static final String PROP_CODE_APPOINTMENT_BOOKING_DATE = "DM_AppointmentBookingDate";

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Appointment;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Appointment model = params.getOutputContainer().getAppointments();

        Appointment fhir = (Appointment) resourceWrapper.getResource(); //returns null if deleted
        AppointmentAdditional appointmentAdditional = params.getOutputContainer().getAppointmentAdditional();
        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {
            model.writeDelete(subscriberId);

            if (params.isIncludeAppointmentAdditional()) {
                appointmentAdditional.writeDelete(subscriberId);
            }
            return;
        }

        long organizationId;
        long patientId;
        long personId;
        Long practitionerId = null;
        Long scheduleId = null;
        Date startDate = null;
        Integer plannedDuration = null;
        Integer actualDuration = null;
        Integer appointmentStatusConceptId;
        Integer patientWait = null;
        Integer patientDelay = null;
        Date sentIn = null;
        Date left = null;
        String sourceId = null;
        Date cancelledDate = null;

        if (fhir.hasParticipant()) {
            for (Appointment.AppointmentParticipantComponent participantComponent : fhir.getParticipant()) {
                Reference reference = participantComponent.getActor();
                ReferenceComponents components = ReferenceHelper.getReferenceComponents(reference);

                if (components.getResourceType() == ResourceType.Practitioner) {
                    practitionerId = transformOnDemandAndMapId(reference, SubscriberTableId.PRACTITIONER, params);
                    break; //break out so we only use the FIRST practitioner
                }
            }
        }

        //the test pack has data that refers to deleted or missing patients, so if we get a null
        //patient ID here, then skip this resource
        if (params.getSubscriberPatientId() == null) {
            LOG.warn("Skipping " + fhir.getResourceType() + " " + fhir.getId() + " as no Enterprise patient ID could be found for it");
            return;
        }

        Extension additionalExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ADDITIONAL);
        Extension bookingDateExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.APPOINTMENT_BOOKING_DATE);
        if (fhir.hasType() || additionalExtension != null || bookingDateExtension != null) {
            if (params.isIncludeAppointmentAdditional()) {
                transformToAppointmentAdditionals(subscriberId, fhir, appointmentAdditional);
            }
        }
        organizationId = params.getSubscriberOrganisationId().longValue();
        patientId = params.getSubscriberPatientId().longValue();
        personId = params.getSubscriberPersonId().longValue();

        if (fhir.getSlot().size() > 1) {
            throw new TransformException("Cannot handle appointments linked to multiple slots " + fhir.getId());
        }
        if (fhir.getSlot().size() > 0) {
            Reference slotReference = fhir.getSlot().get(0);
            Slot fhirSlot = (Slot) params.findOrRetrieveResource(slotReference);
            if (fhirSlot != null) {

                Reference scheduleReference = fhirSlot.getSchedule();
                scheduleId = transformOnDemandAndMapId(scheduleReference, SubscriberTableId.SCHEDULE, params);

            } else {
                //a bug was found that meant this happened. So if it happens again, something is wrong
                throw new TransformException("Failed to find " + slotReference.getReference() + " for " + fhir.getResourceType() + " " + fhir.getId());
                //LOG.warn("Failed to find " + slotReference.getReference() + " for " + fhir.getResourceType() + " " + fhir.getId());
            }
        }

        startDate = fhir.getStart();

        Date end = fhir.getEnd();
        if (startDate != null && end != null) {
            long millisDiff = end.getTime() - startDate.getTime();
            plannedDuration = Integer.valueOf((int) (millisDiff / (1000 * 60)));
        }

        if (fhir.hasMinutesDuration()) {
            int duration = fhir.getMinutesDuration();
            actualDuration = Integer.valueOf(duration);
        }

        Appointment.AppointmentStatus status = fhir.getStatus();
        appointmentStatusConceptId = IMHelper.getIMConcept(params, fhir, IMConstant.FHIR_APPOINTMENT_STATUS, status.toCode(), status.getDisplay());

        if (fhir.hasExtension()) {
            for (Extension extension : fhir.getExtension()) {

                if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_PATIENT_WAIT)) {
                    Duration d = (Duration) extension.getValue();
                    if (!d.getUnit().equalsIgnoreCase("minutes")) {
                        throw new TransformException("Unsupported patient wait unit [" + d.getUnit() + "] in " + fhir.getId());
                    }
                    int i = d.getValue().intValue();
                    patientWait = Integer.valueOf(i);

                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_PATIENT_DELAY)) {
                    Duration d = (Duration) extension.getValue();
                    if (!d.getUnit().equalsIgnoreCase("minutes")) {
                        throw new TransformException("Unsupported patient delay unit [" + d.getUnit() + "] in " + fhir.getId());
                    }
                    int i = d.getValue().intValue();
                    patientDelay = Integer.valueOf(i);

                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_SENT_IN)) {
                    DateTimeType dt = (DateTimeType) extension.getValue();
                    sentIn = dt.getValue();

                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_LEFT)) {
                    DateTimeType dt = (DateTimeType) extension.getValue();
                    left = dt.getValue();

                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_ORIGINAL_IDENTIFIER)) {

                    StringType orig_id = (StringType) extension.getValue();
                    //Identifier orig_id = (Identifier) extension.getValue(); //AppointmentBuilder uses String
                    sourceId = orig_id.getValue();

                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_CANCELLATION_DATE)) {
                    DateTimeType dt = (DateTimeType) extension.getValue();
                    cancelledDate = dt.getValue();
                }
            }
        }

        model.writeUpsert(subscriberId,
                organizationId,
                patientId,
                personId,
                practitionerId,
                scheduleId,
                startDate,
                plannedDuration,
                actualDuration,
                appointmentStatusConceptId,
                patientWait,
                patientDelay,
                sentIn,
                left,
                sourceId,
                cancelledDate);

    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.APPOINTMENT;
    }

    private void transformToAppointmentAdditionals(SubscriberId subscriberId, Appointment fhir,
                                                   AppointmentAdditional appointmentAdditional) throws Exception {

        //Transform the text or coded appointment type
        if (fhir.hasType()) {
            Integer propertyConceptDbid = IMHelper.getIMConcept(IMConstant.DISCOVERY_CODE, PROP_CODE_APPOINTMENT_TYPE);
            if (propertyConceptDbid == null) {
                throw new Exception("Got a null concept ID for property code [" + PROP_CODE_APPOINTMENT_TYPE + "] and scheme " + IMConstant.DISCOVERY_CODE);
            }
            String appointmentType = "";
            Integer valueId = null;
            if (fhir.getType().hasCoding()) {
//            appointmentType = fhir.getType().getCoding().get(0).getCode();
//            valueID = Integer.parseInt(appointmentType);
                //for if/when coded concepts come in
            } else {
                appointmentType = fhir.getType().getText();
            }

            appointmentAdditional.writeUpsert(subscriberId, propertyConceptDbid, valueId, null, appointmentType);
        }

        //Transform each additional extension parameter to the appointment_additional data
        Extension additionalExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ADDITIONAL);
        if (additionalExtension != null) {

            Reference idReference = (Reference) additionalExtension.getValue();
            String idReferenceValue = idReference.getReference();
            idReferenceValue = idReferenceValue.substring(1); //remove the leading "#" char

            for (Resource containedResource : fhir.getContained()) {
                if (containedResource.getId().equals(idReferenceValue)) {

                    //additional extension data is stored as Parameter resources
                    Parameters parameters = (Parameters) containedResource;

                    //get all the entries in the parameters list
                    List<Parameters.ParametersParameterComponent> entries = parameters.getParameter();
                    for (Parameters.ParametersParameterComponent parameter : entries) {

                        // each parameter entry will have a key value pair of either:
                        //  1) TEXT_propertycode property plus TEXT data as a value
                        //  2) JSON_propertycode property plus JSON data as value
                        //  3) IM concept property code plus a CodeableConcept value (code and scheme)
                        if (parameter.hasName() && parameter.hasValue()) {

                            String propertyCode = parameter.getName();

                            //Handle basic TEXT data entries such as FIN number
                            if (propertyCode.startsWith("TEXT_")) {

                                String propertyScheme = IMConstant.DISCOVERY_CODE;

                                //get the IM concept property code
                                propertyCode = propertyCode.replace("TEXT_", "");
                                Integer propertyConceptDbid = IMHelper.getIMConcept(propertyScheme, propertyCode);
                                if (propertyConceptDbid == null) {
                                    LOG.debug("Null value for propertyCode " + propertyCode);
                                }
                                //the value is a StringType storing the Text
                                StringType textValue = (StringType) parameter.getValue();
                                appointmentAdditional.writeUpsert(subscriberId, propertyConceptDbid, null, null, textValue.getValue());

                            } else if (propertyCode.startsWith("JSON_")) {

                                //Handle JSON data entries such as CDS birth details
                                String propertyScheme = IMConstant.DISCOVERY_CODE;

                                //get the IM concept property code
                                propertyCode = propertyCode.replace("JSON_", "");
                                Integer propertyConceptDbid = IMHelper.getIMConcept(propertyScheme, propertyCode);
                                if (propertyConceptDbid == null) {
                                    LOG.debug("Null value for propertyCode " + propertyCode);
                                }
                                //the value is a StringType storing JSON
                                StringType jsonValue = (StringType) parameter.getValue();
                                appointmentAdditional.writeUpsert(subscriberId, propertyConceptDbid, null, jsonValue.getValue(), null);

                            } else {
                                //and finally, these values are from IM API mapping, an IM property_id and value_id pair
                                String propertyScheme = IMConstant.DISCOVERY_CODE;
                                String type = parameter.getValue().getClass().getSimpleName();

                                //only CodeableConcept values should exist
                                if (type.equalsIgnoreCase("CodeableConcept")) {
                                    CodeableConcept parameterValue = (CodeableConcept) parameter.getValue();
                                    String valueCode = parameterValue.getCoding().get(0).getCode();
                                    String valueScheme = parameterValue.getCoding().get(0).getSystem();
                                    //we need to look up DBids for both property and value
                                    Integer propertyConceptDbid = IMHelper.getIMConcept(propertyScheme, propertyCode);
                                    if (propertyConceptDbid == null) {
                                        LOG.debug("No property found for scheme: " + propertyScheme + ":code:" + propertyCode + ".");
                                        LOG.debug("Skipping problem parameter: " + fhir.getId() + ":" + fhir.getResourceType().toString());
                                        continue;
                                    }
                                    Integer valueConceptDbid = IMHelper.getIMConcept(valueScheme, valueCode);

                                    //transform the IM values to the encounter_additional table upsert
                                    appointmentAdditional.writeUpsert(subscriberId, propertyConceptDbid, valueConceptDbid, null, null);
                                }
                                else {
                                    throw new Exception("Unsupported value type [" + type + "] for property code [" + propertyCode + "] and scheme " + propertyScheme);
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }
        Extension bookingDateExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri. APPOINTMENT_BOOKING_DATE);
        if (bookingDateExtension != null) {
            Integer propertyConceptDbid = IMHelper.getIMConcept(IMConstant.DISCOVERY_CODE,
                    PROP_CODE_APPOINTMENT_BOOKING_DATE);
            if (propertyConceptDbid == null) {
                throw new Exception("Got a null concept ID for property code [" + PROP_CODE_APPOINTMENT_BOOKING_DATE + "] and " +
                        "scheme " + IMConstant.DISCOVERY_CODE);
            }
            DateTimeType dt = (DateTimeType) bookingDateExtension.getValue();
            String appointmentBookingDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dt.getValue());

            appointmentAdditional.writeUpsert(subscriberId, propertyConceptDbid, null, null, appointmentBookingDate);
        }
    }
}
