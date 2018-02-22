package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsInternalIdDal;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.TransformRuntimeException;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private static Map<Long, PatientBuilder> patientBuilders = new HashMap<>();

    public static PatientBuilder getPatientBuilder(CsvCell millenniumIdCell, BartsCsvHelper csvHelper) throws Exception {
        PatientBuilder patientBuilder = patientBuilders.get(millenniumIdCell.getLong());
        if (patientBuilder == null) {

            //first look up the MRN for the person ID
            InternalIdDalI internalIdDalI = DalProvider.factoryInternalIdDal();
            String mrn = internalIdDalI.getDestinationId(csvHelper.getServiceId(), RdbmsInternalIdDal.IDTYPE_MRN_MILLENNIUM_PERS_ID, millenniumIdCell.getString());
            if (mrn == null) {
                throw new TransformRuntimeException("MRN not found for PersonId " + millenniumIdCell.getString());
            }

            ResourceId patientResourceId = BasisTransformer.getPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, csvHelper.getPrimaryOrgHL7OrgOID(), mrn);
            if (patientResourceId == null) {

                patientBuilder = new PatientBuilder();

                patientResourceId = BasisTransformer.createPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, csvHelper.getPrimaryOrgHL7OrgOID(), mrn);
                patientBuilder.setId(patientResourceId.getResourceId().toString());

            } else {
                Patient patient = (Patient)csvHelper.retrieveResource(patientResourceId.getResourceId().toString(), ResourceType.Patient);
                patientBuilder = new PatientBuilder(patient);
            }

            patientBuilder = patientBuilders.put(millenniumIdCell.getLong(), patientBuilder);
        }
        return patientBuilder;
    }

    public static void filePatientResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Long milleniumId: patientBuilders.keySet()) {
            PatientBuilder patientBuilder = patientBuilders.get(milleniumId);
            BasisTransformer.savePatientResource(fhirResourceFiler, null, patientBuilder);
        }

        //there should be no attempt to reference this cache after this point, so set to null
        //to ensure any attempt results in an exception
        patientBuilders = null;
    }
}
