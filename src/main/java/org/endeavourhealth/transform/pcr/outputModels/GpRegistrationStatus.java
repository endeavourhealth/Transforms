package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;
 
public class GpRegistrationStatus extends AbstractPcrCsvWriter {
 
 
 
  public GpRegistrationStatus(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long patientId,
                    Long owningOrganisationId,
                    Date effectiveDate,
                    Integer effectiveDatePrecision,
                    Long effectivePractitionerId,
                    Long enteredByPractitionerId,
                    Date endDate,
                    Integer gpRegistrationTypeConceptId,
                    Integer gpRegistrationStatusConceptId,
                    Integer gpRegistrationStatusSubConceptId,
Boolean isCurrent
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(patientId),
                    convertLong(owningOrganisationId),
                    convertDate(effectiveDate),
                    convertInt(effectiveDatePrecision),
                    convertLong(effectivePractitionerId),
                    convertLong(enteredByPractitionerId),
                    convertDate(endDate),
                    convertInt(gpRegistrationTypeConceptId),
                    convertInt(gpRegistrationStatusConceptId),
                    convertInt(gpRegistrationStatusSubConceptId),
                    convertBoolean(isCurrent)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "patient_id",
                       "owning_organisation_id",
                       "effective_date",
                       "effective_date_precision",
                       "effective_practitioner_id",
                       "entered_by_practitioner_id",
                       "end_date",
                       "gp_registration_type_concept_id",
                       "gp_registration_status_concept_id",
                       "gp_registration_status_sub_concept_id",
                     "is_current"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    String.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Integer.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Integer.class,
                    Integer.class,
                    Integer.class,
                    Boolean.class
    }; 
}
}
