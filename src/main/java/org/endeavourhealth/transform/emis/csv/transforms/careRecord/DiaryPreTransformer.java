package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.exceptions.EmisCodeNotFoundException;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.appointment.Slot;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Diary;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;

public class DiaryPreTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        Diary parser = (Diary)parsers.get(Diary.class);
        while (parser != null && parser.nextRecord()) {
            try {
                createResource(parser, fhirResourceFiler, csvHelper);

            } catch (Exception ex) {
                //because this is a pre-transform to cache data, if we have any exceptions, don't continue - just throw it up
                throw new TransformException(parser.getCurrentState().toString(), ex);
            }
        }
    }

    private static void createResource(Diary parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper) throws Exception {

        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            return;
        }

        CsvCell consultationGuid = parser.getConsultationGuid();
        if (!consultationGuid.isEmpty()) {
            CsvCell diaryGuid = parser.getDiaryGuid();
            CsvCell patientGuid = parser.getPatientGuid();

            csvHelper.cacheNewConsultationChildRelationship(consultationGuid,
                    patientGuid,
                    diaryGuid,
                    ResourceType.ProcedureRequest);
        }
    }

}
