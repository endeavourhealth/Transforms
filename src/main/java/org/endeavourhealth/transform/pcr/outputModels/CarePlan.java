package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class CarePlan extends AbstractPcrCsvWriter {
 
 
 
  public CarePlan(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long patientId,
                    Long conceptId,
                    Date effectiveDate,
                    Integer effectiveDatePrecision,
                    Long effectivePractitionerId,
                    Long enteredByPractitionerId,
                    Date endDate,
                    Long careActivityId,
                    Long careActivityHeadingConceptId,
                    Long owningOrganisationId,
                    Long statusConceptId,
                    Boolean isConfidential,
                    Long descriptionFreeTextId,
                    Long performanceFrequencyValue,
                    Integer performanceFrequencyUnit,
                    Long performanceLocationConceptId,
                    Long parentCarePlan,
                    Long followUpEventId,
Boolean isConsent
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(patientId),
                    convertLong(conceptId),
                    convertDate(effectiveDate),
                    convertInt(effectiveDatePrecision),
                    convertLong(effectivePractitionerId),
                    convertLong(enteredByPractitionerId),
                    convertDate(endDate),
                    convertLong(careActivityId),
                    convertLong(careActivityHeadingConceptId),
                    convertLong(owningOrganisationId),
                    convertLong(statusConceptId),
                    convertBoolean(isConfidential),
                    convertLong(descriptionFreeTextId),
                    convertLong(performanceFrequencyValue),
                    convertInt(performanceFrequencyUnit),
                    convertLong(performanceLocationConceptId),
                    convertLong(parentCarePlan),
                    convertLong(followUpEventId),
                    convertBoolean(isConsent)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "patient_id",
                       "concept_id",
                       "effective_date",
                       "effective_date_precision",
                       "effective_practitioner_id",
                       "entered_by_practitioner_id",
                       "end_date",
                       "care_activity_id",
                       "care_activity_heading_concept_id",
                       "owning_organisation_id",
                       "status_concept_id",
                       "is_confidential",
                       "description_free_text_id",
                       "performance_frequency_value",
                       "performance_frequency_unit",
                       "performance_location_concept_id",
                       "parent_care_plan",
                       "follow_up_event_id",
                     "is_consent"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Long.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Integer.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Boolean.class,
                    Long.class,
                    Long.class,
                    Integer.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Boolean.class
    }; 
}
}
