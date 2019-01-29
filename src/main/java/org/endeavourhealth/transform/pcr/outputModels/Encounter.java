package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;
 
public class Encounter extends AbstractPcrCsvWriter {

    //TODO Auto-generated. May need some long/int tweaking etc
 
  public Encounter(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long patientId,
                    Long owningOrganisationId,
                    Long careEpisodeId,
                    Long parentEncounterId,
                    Date startDate,
                    Date endDate,
                    String eventType,
                    String state,
                    Long practitionerId,
                    String originalEncounterRef,
                    Long appointmentSlotId,
                    String additionalData,
Long locationId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(patientId),
                    convertLong(owningOrganisationId),
                    convertLong(careEpisodeId),
                    convertLong(parentEncounterId),
                    convertDate(startDate),
                    convertDate(endDate),
                    eventType,
                    state,
                    convertLong(practitionerId),
                    originalEncounterRef,
                    convertLong(appointmentSlotId),
                    additionalData,
                    convertLong(locationId)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "patient_id",
                       "owning_organisation_id",
                       "care_episode_id",
                       "parent_encounter_id",
                       "start_date",
                       "end_date",
                       "event_type",
                       "state",
                       "practitioner_id",
                       "original_encounter_ref",
                       "appointment_slot_id",
                       "additional_data",
                     "location_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    String.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Date.class,
                    String.class,
                    String.class,
                    Long.class,
                    String.class,
                    Long.class,
                    String.class,
                    Long.class
    }; 
}
}
