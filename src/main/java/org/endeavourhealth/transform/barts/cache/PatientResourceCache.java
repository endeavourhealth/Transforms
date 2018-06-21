package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private Map<Long, PatientBuilder> patientBuildersByPersonId = new HashMap<>();

    public PatientBuilder getPatientBuilder(CsvCell personIdCell, BartsCsvHelper csvHelper) throws Exception {

        Long personId = personIdCell.getLong();
        return getPatientBuilder(personId, csvHelper);
    }

    public PatientBuilder getPatientBuilder(Long personId, BartsCsvHelper csvHelper) throws Exception {

        //check the cache first
        PatientBuilder patientBuilder = patientBuildersByPersonId.get(personId);
        if (patientBuilder == null) {

            //each of the patient transforms only updates part of the FHIR resource, so we need to retrieve any existing instance to update
            Patient patient = (Patient)csvHelper.retrieveResourceForLocalId(ResourceType.Patient, personId.toString());
            if (patient == null) {
                //if the patient doesn't exist yet, create a new one
                patientBuilder = new PatientBuilder();
                patientBuilder.setId(personId.toString());

                //always set the managing organisation to Barts
                String bartsId = csvHelper.findOrgRefIdForBarts();
                Reference organisationReference = ReferenceHelper.createReference(ResourceType.Organization, bartsId);
                patientBuilder.setManagingOrganisation(organisationReference);

                //for new patients, put the Person ID as an identifier on the resource
                //create the Identity builder, which will generate a new one if the existing variable is still null
                IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
                identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
                identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_INTERNAL_PERSON);
                identifierBuilder.setValue(personId.toString());

            } else {

                patientBuilder = new PatientBuilder(patient);
            }

            patientBuildersByPersonId.put(personId, patientBuilder);
        }

        return patientBuilder;
    }

    public void filePatientResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.trace("Saving " + patientBuildersByPersonId.size() + " patients to the DB");

        for (Long personId: patientBuildersByPersonId.keySet()) {
            PatientBuilder patientBuilder = patientBuildersByPersonId.get(personId);

            boolean performIdMapping = !patientBuilder.isIdMapped();
            fhirResourceFiler.savePatientResource(null, performIdMapping, patientBuilder);
        }

        LOG.trace("Finishing saving " + patientBuildersByPersonId.size() + " patients to the DB");

        //clear down as everything has been saved
        patientBuildersByPersonId.clear();
    }

    /*public static PatientBuilder getPatientBuilder(CsvCell milleniumPersonIdCell, BartsCsvHelper csvHelper) throws Exception {

        UUID patientId = csvHelper.findPatientIdFromPersonId(milleniumPersonIdCell);

        //if we don't know the Person->MRN mapping, then the UUID returned will be null, in which case we can't proceed
        if (patientId == null) {
            //LOG.trace("Failed to find patient UUID for person ID " + milleniumPersonIdCell.getString());
            return null;
        }

        PatientBuilder patientBuilder = patientBuildersByUuid.get(patientId);
        if (patientBuilder == null) {

            //each of the patient transforms only updates part of the FHIR resource, so we need to retrieve any existing instance to update
            Patient patient = (Patient)csvHelper.retrieveResource(ResourceType.Patient, patientId);
            if (patient == null) {
                //if the patient doesn't exist yet, create a new one
                patientBuilder = new PatientBuilder();
                patientBuilder.setId(patientId.toString());

            } else {

                patientBuilder = new PatientBuilder(patient);

                //due to a previous bug in the transform, we've saved a load of Patient resources without an ID, so fix this now
                *//*if (Strings.isNullOrEmpty(patientBuilder.getResourceId())) {
                    patientBuilder.setId(patientId.toString());
                    //throw new TransformRuntimeException("Retrieved patient " + patientResourceId.getResourceId() + " from DB and it has no ID");
                }*//*
            }

            patientBuildersByUuid.put(patientId, patientBuilder);
        }
        return patientBuilder;
    }

    public static void filePatientResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.trace("Saving " + patientBuildersByUuid.size() + " patients to the DB");

        for (UUID patientId: patientBuildersByUuid.keySet()) {
            PatientBuilder patientBuilder = patientBuildersByUuid.get(patientId);
            BasisTransformer.savePatientResource(fhirResourceFiler, null, patientBuilder);
        }

        LOG.trace("Finishing saving " + patientBuildersByUuid.size() + " patients to the DB");

        //clear down as everything has been saved
        patientBuildersByUuid.clear();
    }*/
}
