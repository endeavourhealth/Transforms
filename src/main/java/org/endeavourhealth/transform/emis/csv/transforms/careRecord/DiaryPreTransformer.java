package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.core.exceptions.CodeNotFoundException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Diary;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;

public class DiaryPreTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Diary.class);
       while (parser != null && parser.nextRecord()) {
          try {
                createResource((Diary)parser, fhirResourceFiler, csvHelper);
            } catch (CodeNotFoundException ex) {
               String errorRecClsName = Thread.currentThread().getStackTrace()[1].getClassName();
                csvHelper.logErrorRecord(ex,((Diary) parser).getPatientGuid(),((Diary) parser).getDiaryGuid(),errorRecClsName);
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
