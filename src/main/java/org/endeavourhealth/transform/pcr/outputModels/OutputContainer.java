package org.endeavourhealth.transform.pcr.outputModels;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.core.exceptions.TransformException;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

//import org.endeavourhealth.transform.pcr.outputModels.UnusedSoFar.Flag;

public class OutputContainer {

    public static final String UPSERT = "Upsert";
    public static final String DELETE = "Delete";

    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String TIME_FORMAT = "hh:mm:ss";
    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT;

    private static final String COLUMN_CLASS_MAPPINGS = "ColumnClassMappings.json";
    
    private final List<AbstractPcrCsvWriter> csvWriters;


    public OutputContainer(boolean pseduonymised) throws Exception {
        this(CSV_FORMAT, DATE_FORMAT, TIME_FORMAT, pseduonymised);
    }

    public OutputContainer(CSVFormat csvFormat, String dateFormat, String timeFormat, boolean pseduonymised) throws Exception {

        //NOTE: file names match to database table names

        csvWriters = new ArrayList<>();
        csvWriters.add(new Organisation("organisation.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Location("location.csv", csvFormat, dateFormat, timeFormat));

        //TODO - need LocationContact

        csvWriters.add(new Practitioner("practitioner.csv", csvFormat, dateFormat, timeFormat));
        //csvWriters.add(new Schedule("schedule.csv", csvFormat, dateFormat, timeFormat));
        //csvWriters.add(new Person("person.csv", csvFormat, dateFormat, timeFormat, pseduonymised));
        csvWriters.add(new Patient("patient.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new PatientAddress("patient_address.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Address("address.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new PatientContact("patient_contact.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new PatientIdentifier("patient_identifier.csv", csvFormat, dateFormat, timeFormat));

        //TODO - need EventRelationship

        //csvWriters.add(new EpisodeOfCare("episode_of_care.csv", csvFormat, dateFormat, timeFormat));
        //csvWriters.add(new Appointment("appointment.csv", csvFormat, dateFormat, timeFormat));
        //csvWriters.add(new Encounter("encounter.csv", csvFormat, dateFormat, timeFormat));
        //csvWriters.add(new EncounterDetail("encounter_detail.csv", csvFormat, dateFormat, timeFormat));
        //csvWriters.add(new EncounterRaw("encounter_raw.csv", csvFormat, dateFormat, timeFormat));
        //csvWriters.add(new Flag("flag.csv", csvFormat, dateFormat, timeFormat));
        //csvWriters.add(new ReferralRequest("referral_request.csv", csvFormat, dateFormat, timeFormat));
        //csvWriters.add(new ProcedureRequest("procedure_request.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Observation("observation.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new ObservationValue("observation_value.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new MedicationStatement("medication_statement.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new MedicationOrder("medication_order.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new MedicationAmount("medication_amount.csv", csvFormat, dateFormat, timeFormat));

        //TODO: - need free text table links

        csvWriters.add(new Allergy("allergy.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Immunisation("immunisation.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Problem("problem.csv", csvFormat, dateFormat, timeFormat));
        //csvWriters.add(new LinkDistributor("link_distributor.csv", csvFormat, dateFormat, timeFormat));
    }

    public byte[] writeToZip() throws Exception {

        //may as well zip the data, since it will compress well
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(baos);
        
        //the first entry is a json file giving us the target class names for each column
        ObjectNode columnClassMappingJson = new ObjectNode(JsonNodeFactory.instance);

        for (AbstractPcrCsvWriter csvWriter: csvWriters) {
            writeColumnClassMappings(csvWriter, columnClassMappingJson);
        }

        String jsonStr = ObjectMapperPool.getInstance().writeValueAsString(columnClassMappingJson);
        zos.putNextEntry(new ZipEntry(COLUMN_CLASS_MAPPINGS));
        zos.write(jsonStr.getBytes());
        zos.flush();
        
        //then write the CSV files
        for (AbstractPcrCsvWriter csvWriter: csvWriters) {
            writeZipEntry(csvWriter, zos);
        }

        //close
        zos.close();

        //return as base64 encoded string
        return baos.toByteArray();

    }
    
    private static void writeColumnClassMappings(AbstractPcrCsvWriter csvWriter, ObjectNode columnClassMappingJson) throws Exception {

        //we only write CSV files with rows, so don't bother writing their column mappings either
        if (csvWriter.isEmpty()) {
            return;
        }

        String fileName = csvWriter.getFileName();

        //write out the column object mappings
        String[] columnNames = csvWriter.getCsvHeaders();
        Class[] classes = csvWriter.getColumnTypes();
        if (columnNames.length != classes.length) {
            throw new TransformException("Column names array (" + columnNames.length + ") isn't same length as classes array (" + classes.length + ") for " + csvWriter.getFileName());
        }

        ObjectNode jsonObject = columnClassMappingJson.putObject(fileName);

        for (int i=0; i<columnNames.length; i++) {
            String columnName = columnNames[i];
            Class cls = classes[i];
            jsonObject.put(columnName, cls.getName());
        }
    }

    private static void writeZipEntry(AbstractPcrCsvWriter csvWriter, ZipOutputStream zipOutputStream) throws Exception {

        //don't bother writing empty CSV files
        if (csvWriter.isEmpty()) {
            return;
        }

        byte[] bytes = csvWriter.close();
        String fileName = csvWriter.getFileName();

        zipOutputStream.putNextEntry(new ZipEntry(fileName));
        zipOutputStream.write(bytes);
        zipOutputStream.flush();
    }

    public List<AbstractPcrCsvWriter> getCsvWriters() {
        return csvWriters;
    }

    @SuppressWarnings("unchecked")
    public <T extends AbstractPcrCsvWriter> T findCsvWriter(Class<T> cls) {
        for (AbstractPcrCsvWriter csvWriter: csvWriters) {
            if (csvWriter.getClass() == cls) {
                return (T)csvWriter;
            }
        }
        return null;
    }

    public Organisation getOrganisations() {
        return findCsvWriter(Organisation.class);
    }

    public Immunisation getImmunisations() { return findCsvWriter(Immunisation.class);}

    public Location getLocations() {
        return findCsvWriter(Location.class);
    }

    public Practitioner getPractitioners() {
        return findCsvWriter(Practitioner.class);
    }

  //  public Schedule getSchedules() {
  //     return findCsvWriter(Schedule.class);
  //  }

 //   public Person getPersons() {
   //     return findCsvWriter(Person.class);
  //  }

    public Patient getPatients() {
        return findCsvWriter(Patient.class);
    }

    public CareEpisode getEpisodesOfCare() {
        return findCsvWriter(CareEpisode.class);
    }
//TODO  appointments not in this build
//    public Appointment getAppointments() {
//        return findCsvWriter(Appointment.class);
//    }

//    public Encounter getEncounters() {
//        return findCsvWriter(Encounter.class);
//    }
//TODO what's decision on how Encounters work?
//    public EncounterDetail getEncounterDetails() {
//        return findCsvWriter(EncounterDetail.class);
//    }
//
//    public EncounterRaw getEncounterRaws() {
//        return findCsvWriter(EncounterRaw.class);
//    }

//    public org.endeavourhealth.transform.pcr.outputModels.Flag getFlags() {
//        return findCsvWriter(Flag.class);
//    }

  //  public ReferralRequest getReferralRequests() {
 //       return findCsvWriter(ReferralRequest.class);
  //  }

//    public ProcedureRequest getProcedureRequests() {
//        return findCsvWriter(ProcedureRequest.class);
//    }

    public Observation getObservations() {
        return findCsvWriter(Observation.class);
    }

    public ObservationValue getObservationValues() {
        return findCsvWriter(ObservationValue.class);
    }

    public MedicationStatement getMedicationStatements() {
        return findCsvWriter(MedicationStatement.class);
    }

    public MedicationOrder getMedicationOrders() {
        return findCsvWriter(MedicationOrder.class);
    }

    public MedicationAmount getMedicationAmounts() {
        return findCsvWriter(MedicationAmount.class);
    }

    public Problem getProblems() {
        return findCsvWriter(Problem.class);
    }
    public PatientAddress getPatientAddresses() {
        return findCsvWriter(PatientAddress.class);
    }
    public PatientIdentifier getPatientIdentifiers() {
        return findCsvWriter(PatientIdentifier.class);
    }
    public PatientContact getPatientContacts() {
        return findCsvWriter(PatientContact.class);
    }
    public Allergy getAllergyIntolerances() {
        return findCsvWriter(Allergy.class);
    }

//    public LinkDistributor getLinkDistributors() {
//        return findCsvWriter(LinkDistributor.class);
//    }
}
