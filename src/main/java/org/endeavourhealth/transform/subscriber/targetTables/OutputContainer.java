package org.endeavourhealth.transform.subscriber.targetTables;

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

    /*static final String UPSERT = "Upsert";
    static final String DELETE = "Delete";*/

    //default to SQL compatible dates and CSV format
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String TIME_FORMAT = "hh:mm:ss";
    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT;

    private static final String COLUMN_CLASS_MAPPINGS = "ColumnClassMappings.json";
    
    private final List<AbstractTargetTable> csvWriters;


    public OutputContainer() throws Exception {
        this(CSV_FORMAT, DATE_FORMAT, TIME_FORMAT);
    }

    public OutputContainer(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {

        csvWriters = new ArrayList<>();
        csvWriters.add(new Organization(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Location(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Practitioner(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Schedule(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Patient(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new EpisodeOfCare(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Appointment(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Encounter(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new EncounterEvent(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Flag(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new ReferralRequest(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new ProcedureRequest(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new Observation(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new MedicationStatement(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new MedicationOrder(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new AllergyIntolerance(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new PseudoId(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new PatientContact(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new PatientAddress(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new DiagnosticOrder(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new PatientAddressMatch(csvFormat, dateFormat, timeFormat));
        csvWriters.add(new RegistrationStatusHistory(csvFormat, dateFormat, timeFormat));
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

        for (AbstractTargetTable csvWriter: csvWriters) {
            writeColumnClassMappings(csvWriter, columnClassMappingJson);
        }

        String jsonStr = ObjectMapperPool.getInstance().writeValueAsString(columnClassMappingJson);
        zos.putNextEntry(new ZipEntry(COLUMN_CLASS_MAPPINGS));
        zos.write(jsonStr.getBytes());
        zos.flush();

        //then write the CSV files
        for (AbstractTargetTable csvWriter: csvWriters) {
            writeZipEntry(csvWriter, zos);
        }

        //close
        zos.close();

        //return as base64 encoded string
        return baos.toByteArray();
    }

    private boolean isEmpty() {

        //if any writer is not empty, return false
        for (AbstractTargetTable csvWriter: csvWriters) {
            if (!csvWriter.isEmpty()) {
                return false;
            }
        }

        return true;
    }

    public void clearDownOutputContainer(List<SubscriberTableId> filesToKeep) throws Exception {
        this.csvWriters.removeIf(c -> !filesToKeep.contains(c.getTableId()));
    }

    private static void writeColumnClassMappings(AbstractTargetTable csvWriter, ObjectNode columnClassMappingJson) throws Exception {

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

    private static void writeZipEntry(AbstractTargetTable csvWriter, ZipOutputStream zipOutputStream) throws Exception {

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

    public List<AbstractTargetTable> getCsvWriters() {
        return csvWriters;
    }

    @SuppressWarnings("unchecked")
    public <T extends AbstractTargetTable> T findCsvWriter(Class<T> cls) {
        for (AbstractTargetTable csvWriter: csvWriters) {
            if (csvWriter.getClass() == cls) {
                return (T)csvWriter;
            }
        }
        return null;
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

    public EncounterEvent getEncounterEvents() {
        return findCsvWriter(EncounterEvent.class);
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

    public PseudoId getPseudoIds() {
        return findCsvWriter(PseudoId.class);
    }

    public PatientContact getPatientContacts() {
        return findCsvWriter(PatientContact.class);
    }

    public PatientAddress getPatientAddresses() {
        return findCsvWriter(PatientAddress.class);
    }

    public DiagnosticOrder getDiagnosticOrder() {
        return findCsvWriter(DiagnosticOrder.class);
    }

    public PatientAddressMatch getPatientAddressMatch() {
        return findCsvWriter(PatientAddressMatch.class);
    }

    public RegistrationStatusHistory getRegistrationStatusHistory () { return findCsvWriter(RegistrationStatusHistory .class); }
}
