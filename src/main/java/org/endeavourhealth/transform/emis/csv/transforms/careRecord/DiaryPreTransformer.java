package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.core.exceptions.RecordNotFoundException;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Consultation;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Diary;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Observation;
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
            } catch (RecordNotFoundException ex) {
                String codeIdString= ex.getMessage();
                String errorRecClsName = Thread.currentThread().getStackTrace()[1].getClassName();
                codeIdString = codeIdString.contains(":") ? codeIdString.split(":")[1] :codeIdString;
                csvHelper.logErrorRecord(Long.parseLong(codeIdString),((Diary) parser).getPatientGuid(),((Diary) parser).getDiaryGuid(),errorRecClsName);
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
