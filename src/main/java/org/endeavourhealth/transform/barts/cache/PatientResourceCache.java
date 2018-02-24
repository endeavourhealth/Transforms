package org.endeavourhealth.transform.barts.cache;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
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
import java.util.List;
import java.util.Map;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private static Map<Long, PatientBuilder> patientBuilders = new HashMap<>();

    public static PatientBuilder getPatientBuilder(CsvCell millenniumIdCell, BartsCsvHelper csvHelper) throws Exception {
        PatientBuilder patientBuilder = patientBuilders.get(millenniumIdCell.getLong());
        if (patientBuilder == null) {

            //first look up the MRN for the person ID
            InternalIdDalI internalIdDalI = DalProvider.factoryInternalIdDal();
            List<InternalIdMap> mrnMaps = internalIdDalI.getSourceId(csvHelper.getServiceId(), RdbmsInternalIdDal.IDTYPE_MRN_MILLENNIUM_PERS_ID, millenniumIdCell.getString());
            if (mrnMaps.isEmpty()) {
                throw new TransformRuntimeException("MRN not found for PersonId " + millenniumIdCell.getString());
            }

            //TODO - confirm this is right. When finding an MRN for the Millennium Person ID it uses the one MOST RECENTLY updated. Is this right?

            //always use the most recent MRN for the person
            mrnMaps.sort((o1, o2) -> (o1.getUpdatedAt().compareTo(o2.getUpdatedAt())));

            int size = mrnMaps.size();
            InternalIdMap lastMap = mrnMaps.get(size-1);
            String mrn = lastMap.getSourceId();

            ResourceId patientResourceId = BasisTransformer.getPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, csvHelper.getPrimaryOrgHL7OrgOID(), mrn);
            if (patientResourceId == null) {

                patientBuilder = new PatientBuilder();

                patientResourceId = BasisTransformer.createPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, csvHelper.getPrimaryOrgHL7OrgOID(), mrn);
                patientBuilder.setId(patientResourceId.getResourceId().toString());

                /*if (Strings.isNullOrEmpty(patientBuilder.getResourceId())) {
                    throw new TransformRuntimeException("Just assigned ID to patient and it's empty");
                }*/

            } else {
                Patient patient = (Patient)csvHelper.retrieveResource(patientResourceId.getResourceId().toString(), ResourceType.Patient);
                patientBuilder = new PatientBuilder(patient);

                //due to a previous bug in the transform, we've saved a load of Patient resources without an ID, so fix this now
                if (Strings.isNullOrEmpty(patientBuilder.getResourceId())) {
                    patientBuilder.setId(patientResourceId.getResourceId().toString());
                    //throw new TransformRuntimeException("Retrieved patient " + patientResourceId.getResourceId() + " from DB and it has no ID");
                }
            }

            patientBuilders.put(millenniumIdCell.getLong(), patientBuilder);
        }
        return patientBuilder;
    }

    public static void filePatientResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.trace("Saving " + patientBuilders.size() + " patients to the DB");

        for (Long milleniumId: patientBuilders.keySet()) {
            PatientBuilder patientBuilder = patientBuilders.get(milleniumId);
            BasisTransformer.savePatientResource(fhirResourceFiler, null, patientBuilder);
        }

        LOG.trace("Finishing saving " + patientBuilders.size() + " patients to the DB");

        //there should be no attempt to reference this cache after this point, so set to null
        //to ensure any attempt results in an exception
        patientBuilders = null;
    }
}
