package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Diary;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;

public class DiaryPreTransformer {

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Diary.class);
        while (parser.nextRecord()) {

            try {
                createResource((Diary)parser, fhirResourceFiler, csvHelper, version);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }


    private static void createResource(Diary parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version) throws Exception {

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
