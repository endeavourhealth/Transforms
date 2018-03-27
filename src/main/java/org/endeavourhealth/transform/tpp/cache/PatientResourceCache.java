package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PatientResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PatientResourceCache.class);

    private static Map<Long, PatientBuilder> PatientBuildersByRowId = new HashMap<>();

    public static PatientBuilder getPatientBuilder(CsvCell rowIdCell, TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        PatientBuilder PatientBuilder = PatientBuildersByRowId.get(rowIdCell.getLong());
        if (PatientBuilder == null) {

            Patient Patient
                    = (Patient)csvHelper.retrieveResource(rowIdCell.getString(), ResourceType.Patient, fhirResourceFiler);
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

    public static void removePatientByRowId(CsvCell rowIdCell, FhirResourceFiler fhirResourceFiler, AbstractCsvParser csvParser) {
        PatientBuilder patientBuilder = PatientBuildersByRowId.get(rowIdCell.getLong());
        try {
            BasisTransformer.deletePatientResource(fhirResourceFiler, csvParser.getCurrentState(), patientBuilder);
        } catch (Exception ex) {
            LOG.error("Error deleting patient :" + rowIdCell.getString() + csvParser.getFilePath() + ex.getMessage());
        }
        PatientBuildersByRowId.remove(rowIdCell.getLong());

    }

    public static boolean patientInCache(CsvCell rowIdCell)  {
        return PatientBuildersByRowId.containsKey(rowIdCell.getLong());
    }

    public static void filePatientResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Long rowId: PatientBuildersByRowId.keySet()) {
            PatientBuilder patientBuilder = PatientBuildersByRowId.get(rowId);
            BasisTransformer.savePatientResource(fhirResourceFiler, null, patientBuilder);
        }

        //clear down as everything has been saved
        PatientBuildersByRowId.clear();
    }
}
