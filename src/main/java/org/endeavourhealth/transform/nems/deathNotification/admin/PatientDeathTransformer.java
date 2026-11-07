package org.endeavourhealth.transform.nems.deathNotification.admin;

import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.nems.NemsXmlHelper;
import org.endeavourhealth.transform.nems.schema.v1.BundleType;
import org.endeavourhealth.transform.nems.schema.v1.EntryType;
import org.endeavourhealth.transform.nems.schema.v1.PatientType;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.endeavourhealth.transform.nems.NemsXmlHelper.*;

/*
                    --> lots of pre-existing example in other transforms for deceased patients, but essentially....
                    --> extract patient identifier and use to populate and locate an existing Patient FHIR record for the service
                    --> if Patient does not exist, log as a transform warning (unable to lookup patient ....)
                    --> if Patient does exist, set the deceasedDateTime, get and end the associated EpisodeOfCare
                           and file the updated Patient FHIR resource
                 */
public class PatientDeathTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientDeathTransformer.class);

    public static void transform(BundleType bundle, FhirResourceFiler processor, NemsXmlHelper xmlHelper) throws Exception {
        for (EntryType entry : bundle.getEntry()) {
            PatientType patient = entry.getResource().getPatient();
            if (patient != null) {
                if (patient.getId() != null) {
                    LOG.info("Beginning death transform for patient: " + patient.getId().getValue());
                    String uniqueId = xmlHelper.getInternalId(xmlHelper.NEMS_BIRTH_UUID_TO_NHS_NO, patient.getIdentifier().getValue().getValue());

                    Object existingResource = null;
                    if(uniqueId != null) {
                        existingResource = xmlHelper.retrieveResource(uniqueId, ResourceType.Patient);
                    }
                    PatientBuilder builder;
                    if (existingResource != null) {
                        builder = new PatientBuilder((Patient)existingResource);

                        xmlHelper.createDOD(builder, patient.getDeceasedDateTime());
                        processor.savePatientResource(null, false, builder);
                    } else {
                        if (null != patient.getIdentifier()) {
                            String internalId = xmlHelper.getInternalId(xmlHelper.NEMS_BIRTH_UUID_TO_NHS_NO, patient.getIdentifier().getValue().getValue());
                            if (internalId != null) {
                                existingResource = xmlHelper.retrieveResource(internalId, ResourceType.Patient);
                                if (existingResource != null) {
                                    builder = new PatientBuilder((Patient)existingResource);

                                    xmlHelper.createDOD(builder, patient.getDeceasedDateTime());
                                    processor.savePatientResource(null, false, builder);
                                }
                            } else {
                                LOG.info("Existing patient resource could not be found for patient ID: " + uniqueId + ". Ignoring notification");
                            }
                        }
                        // No need to register deaths of patients that don't already store resources for.
                        // May be useful in the future.

                        /*builder.setId(uniqueId);

                        DateTime birthDateTime = patient.getBirthDate().getValueAsDateTime();
                        Date dob = birthDateTime.toDate();
                        builder.setDateOfBirth(dob);

                        IdentifierType nationalNumberType = patient.getIdentifier();
                        if (null != nationalNumberType) {
                            //the national number is ALWAYS an NHS number
                            createIdentifier(builder, Identifier.IdentifierUse.OFFICIAL, nationalNumberType.getExtension().getValueCodeableConcept().getCoding().getCode().getValue(), patient.getIdentifier(), processor);
                        }

                        createGender(builder, patient);

                        if(null != patient.getName())
                            createName(builder, patient.getName(), processor);
                        */
                    }
                }
            }
        }
    }
}
