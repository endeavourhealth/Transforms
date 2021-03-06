package org.endeavourhealth.transform.enterprise.outputModels;

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

public class OutputContainer {

    static final String UPSERT = "Upsert";
    static final String DELETE = "Delete";

    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String TIME_FORMAT = "hh:mm:ss";
    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT;

    private static final String COLUMN_CLASS_MAPPINGS = "ColumnClassMappings.json";

    private final List<AbstractEnterpriseCsvWriter> csvWriters;


    public OutputContainer(boolean pseduonymised) throws Exception {
        this(CSV_FORMAT, DATE_FORMAT, TIME_FORMAT, pseduonymised);
    }

    public OutputContainer(CSVFormat csvFormat, String dateFormat, String timeFormat, boolean pseduonymised) throws Exception {

        csvWriters = new ArrayList<>();
        csvWriters.add(new Organization("organization.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Location("location.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Practitioner("practitioner.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Schedule("schedule.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Patient("patient.csv", csvFormat, dateFormat, timeFormat, pseduonymised));
        csvWriters.add(new EpisodeOfCare("episode_of_care.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Appointment("appointment.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Encounter("encounter.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new EncounterDetail("encounter_detail.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new EncounterRaw("encounter_raw.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Flag("flag.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new ReferralRequest("referral_request.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new ProcedureRequest("procedure_request.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Observation("observation.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new MedicationStatement("medication_statement.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new MedicationOrder("medication_order.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new AllergyIntolerance("allergy_intolerance.csv", csvFormat, dateFormat, timeFormat));
        //csvWriters.add(new LinkDistributor("link_distributor.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new PatientAddressMatch("patient_address_match.csv", csvFormat, dateFormat, timeFormat, pseduonymised));
        csvWriters.add(new PatientAddress("patient_address.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new PatientContact("patient_contact.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new RegistrationStatusHistory("registration_status_history.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new EncounterEvent("encounter_event.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new EncounterAdditional("encounter_additional.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new PatientPseudoId("patient_pseudo_id.csv", csvFormat, dateFormat, timeFormat));
        csvWriters.add(new PatientAdditional("patient_additional.csv", csvFormat, dateFormat, timeFormat));

    }

    public byte[] writeToZip() throws Exception {

        //if empty, return null
        if (isEmpty()) {
            return null;
        }

        //may as well zip the data, since it will compress well
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(baos);

        //the first entry is a json file giving us the target class names for each column
        ObjectNode columnClassMappingJson = new ObjectNode(JsonNodeFactory.instance);

        for (AbstractEnterpriseCsvWriter csvWriter : csvWriters) {
            writeColumnClassMappings(csvWriter, columnClassMappingJson);
        }

        String jsonStr = ObjectMapperPool.getInstance().writeValueAsString(columnClassMappingJson);
        zos.putNextEntry(new ZipEntry(COLUMN_CLASS_MAPPINGS));
        zos.write(jsonStr.getBytes());
        zos.flush();

        //then write the CSV files
        for (AbstractEnterpriseCsvWriter csvWriter : csvWriters) {
            writeZipEntry(csvWriter, zos);
        }

        //close
        zos.close();

        //return as base64 encoded string
        return baos.toByteArray();
    }

    private boolean isEmpty() {

        //if any writer is not empty, return false
        for (AbstractEnterpriseCsvWriter csvWriter : csvWriters) {
            if (!csvWriter.isEmpty()) {
                return false;
            }
        }

        return true;
    }

    public void clearDownOutputContainer(List<String> filesToKeep) throws Exception {

        this.csvWriters.removeIf(c -> !filesToKeep.contains(c.getFileNameWithoutExtension()));
    }

    private static void writeColumnClassMappings(AbstractEnterpriseCsvWriter csvWriter, ObjectNode columnClassMappingJson) throws Exception {

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

        for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            Class cls = classes[i];
            jsonObject.put(columnName, cls.getName());
        }
    }

    private static void writeZipEntry(AbstractEnterpriseCsvWriter csvWriter, ZipOutputStream zipOutputStream) throws Exception {

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

    public List<AbstractEnterpriseCsvWriter> getCsvWriters() {
        return csvWriters;
    }

    @SuppressWarnings("unchecked")
    public <T extends AbstractEnterpriseCsvWriter> T findCsvWriter(Class<T> cls) {
        for (AbstractEnterpriseCsvWriter csvWriter : csvWriters) {
            if (csvWriter.getClass() == cls) {
                return (T) csvWriter;
            }
        }
        return null;
    }

    public PatientAddressMatch getPatientAddressMatch() {
        return findCsvWriter(PatientAddressMatch.class);
    }

    public Organization getOrganisations() {
        return findCsvWriter(Organization.class);
    }

    public Location getLocations() {
        return findCsvWriter(Location.class);
    }

    public Practitioner getPractitioners() {
        return findCsvWriter(Practitioner.class);
    }

    public Schedule getSchedules() {
        return findCsvWriter(Schedule.class);
    }

    public Patient getPatients() {
        return findCsvWriter(Patient.class);
    }

    public EpisodeOfCare getEpisodesOfCare() {
        return findCsvWriter(EpisodeOfCare.class);
    }

    public Appointment getAppointments() {
        return findCsvWriter(Appointment.class);
    }

    public Encounter getEncounters() {
        return findCsvWriter(Encounter.class);
    }

    public EncounterDetail getEncounterDetails() {
        return findCsvWriter(EncounterDetail.class);
    }

    public EncounterRaw getEncounterRaws() {
        return findCsvWriter(EncounterRaw.class);
    }

    public Flag getFlags() {
        return findCsvWriter(Flag.class);
    }

    public ReferralRequest getReferralRequests() {
        return findCsvWriter(ReferralRequest.class);
    }

    public ProcedureRequest getProcedureRequests() {
        return findCsvWriter(ProcedureRequest.class);
    }

    public Observation getObservations() {
        return findCsvWriter(Observation.class);
    }

    public MedicationStatement getMedicationStatements() {
        return findCsvWriter(MedicationStatement.class);
    }

    public MedicationOrder getMedicationOrders() {
        return findCsvWriter(MedicationOrder.class);
    }

    public AllergyIntolerance getAllergyIntolerances() {
        return findCsvWriter(AllergyIntolerance.class);
    }

    /*public LinkDistributor getLinkDistributors() {
        return findCsvWriter(LinkDistributor.class);
    }*/

    public PatientAddress getPatientAddresses() {
        return findCsvWriter(PatientAddress.class);
    }

    public PatientContact getPatientContact() {
        return findCsvWriter(PatientContact.class);
    }

    public RegistrationStatusHistory getRegistrationStatusHistory() {
        return findCsvWriter(RegistrationStatusHistory.class);
    }

    public EncounterEvent getEncounterEvent() {
        return findCsvWriter(EncounterEvent.class);
    }

    public EncounterAdditional getEncounterAdditional() {
        return findCsvWriter(EncounterAdditional.class);
    }

    public PatientPseudoId getPatientPseudoId() {
        return findCsvWriter(PatientPseudoId.class);
    }
    public PatientAdditional getPatientAdditional() {
        return findCsvWriter(PatientAdditional.class);
    }

}
