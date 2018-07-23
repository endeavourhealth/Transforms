package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.barts.transformsOld.BasisTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.common.resourceValidators.ResourceValidatorPatient;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private static Map<Long, PatientBuilder> PatientBuildersByRowId = new HashMap<>();

    public static PatientBuilder getOrCreatePatientBuilder(CsvCell rowIdCell, TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        PatientBuilder PatientBuilder = PatientBuildersByRowId.get(rowIdCell.getLong());
        if (PatientBuilder == null) {

            Patient Patient
                    = (Patient) csvHelper.retrieveResource(rowIdCell.getString(), ResourceType.Patient, fhirResourceFiler);
            if (Patient == null) {
                //if the Patient doesn't exist yet, create a new one
                PatientBuilder = new PatientBuilder();
                PatientBuilder.setId(rowIdCell.getString(), rowIdCell);
            } else {
                PatientBuilder = new PatientBuilder(Patient);
            }

            PatientBuildersByRowId.put(rowIdCell.getLong(), PatientBuilder);
        }
        return PatientBuilder;
    }

    public static void removePatientByPatientId(String patientId) {
        PatientBuildersByRowId.remove(Long.getLong(patientId));
    }

    public static void removePatientByRowId(CsvCell rowIdCell, FhirResourceFiler fhirResourceFiler, AbstractCsvParser csvParser) {
        PatientBuilder patientBuilder = PatientBuildersByRowId.get(rowIdCell.getLong());
        try {
            BasisTransformer.deletePatientResource(fhirResourceFiler, csvParser.getCurrentState(), patientBuilder);
        } catch (Exception ex) {
            LOG.error("Error deleting patient :" + rowIdCell.getString() + csvParser.getFilePath() + ex.getMessage());
        }
        PatientBuildersByRowId.remove(rowIdCell.getLong());

    }

    public static boolean patientInCache(CsvCell rowIdCell) {
        return PatientBuildersByRowId.containsKey(rowIdCell.getLong());
    }

    public static void filePatientAndEpisodeOfCareResources(FhirResourceFiler fhirResourceFiler) throws Exception {
        LOG.info("Patient cache count is " + PatientBuildersByRowId.size());

        Iterator iterator = PatientBuildersByRowId.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry both = (Map.Entry)iterator.next();
            Long rowId = (Long) both.getKey();
            PatientBuilder patientBuilder = (PatientBuilder) both.getValue();
            if (EpisodeOfCareResourceCache.episodeOfCareInCache(rowId)) {
                EpisodeOfCareBuilder episodeOfCareBuilder = EpisodeOfCareResourceCache.getEpisodeOfCareByRowId(rowId);
//                LOG.info("Patient: idMapped:" + patientBuilder.isIdMapped() + "<>" + patientBuilder.getResourceId());
//                LOG.info("Episode: idMapped:" + episodeOfCareBuilder.isIdMapped() + "<>" + episodeOfCareBuilder.getResourceId()) ;
                boolean mapIds = !(patientBuilder.isIdMapped() && episodeOfCareBuilder.isIdMapped());
                if (mapIds) {
                    String idString = Long.toString(rowId);
                    if (patientBuilder.isIdMapped()) {
                        patientBuilder.setId(idString);
                    }
                    if (episodeOfCareBuilder.isIdMapped()) {
                        episodeOfCareBuilder.setId(idString);
                    }
                }
                ResourceValidatorPatient validator = new ResourceValidatorPatient();
                List<String> errors = new ArrayList<String>();
                validator.validateResourceDelete(patientBuilder.getResource(),fhirResourceFiler.getServiceId(),mapIds,errors);
                if (errors.isEmpty()) {
                    fhirResourceFiler.savePatientResource(null, mapIds, patientBuilder, episodeOfCareBuilder);
                }
                iterator.remove();
                //PatientBuildersByRowId.remove(rowId);
                EpisodeOfCareResourceCache.removeEpisodeOfCareByPatientId(rowId);
            } else {
                LOG.error("Episode of Care record not found for cached patient record Id: " + rowId);
                //PatientBuildersByRowId.remove(rowId);
                // If a patient doesn't have an EoC something has gone wrong. Leave record for now so we see
                // how big the problem is. Also it's probably a code problem rather than data.
            }
        }
        // Both caches should now be empty
        //TODO - should we do some extra processing?
        LOG.error("At end of filing patient cache count is " + PatientBuildersByRowId.size() + ". Episode of care cache" + EpisodeOfCareResourceCache.size());
        if (PatientBuildersByRowId.size() > 0) {
            LOG.info("Remaining entries in patient cache:");
            for (Long rowId: PatientBuildersByRowId.keySet()) {
                PatientBuilder patientBuilder = PatientBuildersByRowId.get(rowId);
                LOG.info(patientBuilder.toString());
            }
        }
        if (EpisodeOfCareResourceCache.size() > 0) {
            LOG.info("Remaining entries in episode of care cache:");
            EpisodeOfCareResourceCache.listRemaining();
        }
        //LOG.info("Clearing patient and episode of care caches ");
        PatientBuildersByRowId.clear();
        EpisodeOfCareResourceCache.clear();

    }

//    public static void filePatientResources(FhirResourceFiler fhirResourceFiler) throws Exception {
//        LOG.info("Patient cache count is " + PatientBuildersByRowId.size());
//        int count = 0;
//        for (Long rowId : PatientBuildersByRowId.keySet()) {
//            PatientBuilder patientBuilder = PatientBuildersByRowId.get(rowId);
//            fhirResourceFiler.savePatientResource(null, patientBuilder);
//            count++;
//            // if (count % 10000 == 0 ) {
//            //  LOG.info("Patient cache processed " + count + " records");
//            //}
//        }
//        LOG.info("Patient cache processed " + count + " records.");
//
//        //clear down as everything has been saved
//        PatientBuildersByRowId.clear();
//    }
}
