package org.endeavourhealth.transform.homerton.cache;

import org.endeavourhealth.transform.barts.transformsOld.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private static Map<Long, PatientBuilder> patientBuildersByPersonId = new HashMap<>();


    public static PatientBuilder getPatientBuilder(CsvCell milleniumPersonIdCell, HomertonCsvHelper csvHelper) throws Exception {

//        UUID patientId = csvHelper.findPatientIdFromPersonId(milleniumPersonIdCell);
//
//        //if we don't know the Person->MRN mapping, then the UUID returned will be null, in which case we can't proceed
//        if (patientId == null) {
//            //LOG.trace("Failed to find patient UUID for person ID " + milleniumPersonIdCell.getString());
//            return null;
//        }

        Long personId = milleniumPersonIdCell.getLong();
        PatientBuilder patientBuilder = patientBuildersByPersonId.get(personId);
        if (patientBuilder == null) {

            //each of the patient transforms only updates part of the FHIR resource, so we need to retrieve any existing instance to update
            Patient patient = (Patient)csvHelper.retrieveResourceForLocalId(ResourceType.Patient, personId.toString());
            if (patient == null) {
                //if the patient doesn't exist yet, create a new one
                patientBuilder = new PatientBuilder();
                patientBuilder.setId(personId.toString());

            } else {

                patientBuilder = new PatientBuilder(patient);
            }

            patientBuildersByPersonId.put(personId, patientBuilder);
        }
        return patientBuilder;
    }

    public static void filePatientResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.trace("Saving " + patientBuildersByPersonId.size() + " patients to the DB");

        for (Long personId: patientBuildersByPersonId.keySet()) {
            PatientBuilder patientBuilder = patientBuildersByPersonId.get(personId);
            BasisTransformer.savePatientResource(fhirResourceFiler, null, patientBuilder);
        }

        LOG.trace("Finishing saving " + patientBuildersByPersonId.size() + " patients to the DB, clearing cache...");

        //clear down as everything has been saved
        patientBuildersByPersonId.clear();
    }
}
