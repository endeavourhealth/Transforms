package org.endeavourhealth.transform.barts.cache;

import com.google.common.base.Strings;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private static Map<UUID, PatientBuilder> patientBuildersByUuid = new HashMap<>();


    public static PatientBuilder getPatientBuilder(CsvCell milleniumPersonIdCell, BartsCsvHelper csvHelper) throws Exception {

        UUID patientId = csvHelper.findPatientIdFromPersonId(milleniumPersonIdCell);

        //if we don't know the Person->MRN mapping, then the UUID returned will be null, in which case we can't proceed
        if (patientId == null) {
            LOG.trace("Failed to find patient UUID for person ID " + milleniumPersonIdCell.getString());
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
                if (Strings.isNullOrEmpty(patientBuilder.getResourceId())) {
                    patientBuilder.setId(patientId.toString());
                    //throw new TransformRuntimeException("Retrieved patient " + patientResourceId.getResourceId() + " from DB and it has no ID");
                }
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

        //there should be no attempt to reference this cache after this point, so set to null
        //to ensure any attempt results in an exception
        patientBuildersByUuid = null;
    }
}
