package org.endeavourhealth.transform.emis.csv.transforms.appointment;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.common.resourceBuilders.AppointmentBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.appointment.Slot;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 * detects if any previously received Slot records have had their appointments cancelled
 * and updates the FHIR Appointment accordingly
 */
public class SlotPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SlotPreTransformer.class);

    private static final String SLOT_LATEST_PATIENT_GUID = "EmisSlotLatestPatientGuid"; //pipe-delmitted list of patient GUIDs associated with a slot
    private static final String DELIM = "|";

    private static final InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        Map<CsvCell, CsvCell[]> batchUpdates = new HashMap<>(); //slot guid and patient guid cell

        Slot parser = (Slot)parsers.get(Slot.class);
        while (parser != null && parser.nextRecord()) {

            try {
                //SD-289 - don't apply the patient-level filtering here when reprocessing data after a missing code
                //has been fixed, as we end up restoring appointments to "booked" status when they should be cancelled or deleted
                //if (csvHelper.shouldProcessRecord(parser)) {
                processRecord(parser, fhirResourceFiler, csvHelper, batchUpdates);
                //}
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        saveBatches(batchUpdates, true, csvHelper, fhirResourceFiler);

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void processRecord(Slot parser, FhirResourceFiler fhirResourceFiler, EmisCsvHelper csvHelper,
                                     Map<CsvCell, CsvCell[]> batchUpdates) throws Exception {

        CsvCell slotGuid = parser.getSlotGuid();
        CsvCell patientGuid = parser.getPatientGuid();
        CsvCell deletedCell = parser.getDeleted();

        //add to the relevant batch depending on whether deleted or not
        CsvCell[] arr = new CsvCell[]{patientGuid, deletedCell};
        batchUpdates.put(slotGuid, arr);

        saveBatches(batchUpdates, false, csvHelper, fhirResourceFiler);
    }


    private static void saveBatches(Map<CsvCell, CsvCell[]> batchUpdates,
                                    boolean lastOne, EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        //updates batch
        if (!batchUpdates.isEmpty()
                && (lastOne || batchUpdates.size() >= TransformConfig.instance().getResourceSaveBatchSize())) {

            csvHelper.submitToThreadPool(new SlotUpdateCallable(new HashMap<>(batchUpdates), csvHelper, fhirResourceFiler));
            batchUpdates.clear();
        }

        //if we're the last batch, then make sure to wait for everything hit the DB before moving on
        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }


    /**
     * if we detect that the patientGUID has changed (or been cleared), this means the appointment was cancelled,
     * so we need to find the previous Appointment resource and mark it as such.
     */
    private static void cancelExistingAppointment(CsvCell slotGuid, CsvCell patientGuidCell, CsvCell cancellationCauseCell,
                                                  EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        //SD-289, if re-processing exchanges due to missing codes being found, we need to handle slots differently
        //since they're the only record type that can have the patient GUID change. So we process all records, without filtering,
        //and only filter at the point of actually making a change
        if (!csvHelper.getPatientFilter().shouldProcessRecord(patientGuidCell)) {
            return;
        }

        String sourceId = EmisCsvHelper.createUniqueId(patientGuidCell, slotGuid);
        Appointment appointment = (Appointment)csvHelper.retrieveResource(sourceId, ResourceType.Appointment);

        //if the appointment has already been deleted or is cancelled or DNA, then do nothing
        if (appointment == null
                || appointment.getStatus() == Appointment.AppointmentStatus.CANCELLED
                || appointment.getStatus() == Appointment.AppointmentStatus.NOSHOW) {
            return;
        }

        AppointmentBuilder appointmentBuilder = new AppointmentBuilder(appointment);
        appointmentBuilder.setStatus(Appointment.AppointmentStatus.CANCELLED, cancellationCauseCell);

        //save without mapping IDs as this has been retrieved from the DB
        fhirResourceFiler.savePatientResource(null, false, appointmentBuilder);
    }

    /**
     * if a slot is deleted with a patient GUID still in it, then this means the appointment wasn't cancelled first, so delete the FHIR Appointment
     */
    private static void deleteExistingAppointment(CsvCell slotGuid, CsvCell patientGuidCell, CsvCell deleteCauseCell, EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        //SD-289, if re-processing exchanges due to missing codes being found, we need to handle slots differently
        //since they're the only record type that can have the patient GUID change. So we process all records, without filtering,
        //and only filter at the point of actually making a change
        if (!csvHelper.getPatientFilter().shouldProcessRecord(patientGuidCell)) {
            return;
        }

        String sourceId = EmisCsvHelper.createUniqueId(patientGuidCell, slotGuid);
        Appointment appointment = (Appointment)csvHelper.retrieveResource(sourceId, ResourceType.Appointment);

        //if already deleted, return out
        if (appointment == null) {
            return;
        }

        AppointmentBuilder appointmentBuilder = new AppointmentBuilder(appointment);
        appointmentBuilder.setDeletedAudit(deleteCauseCell);

        fhirResourceFiler.deletePatientResource(null, false, appointmentBuilder);
    }


    private static Set<String> getPatientGuidsFromMappingStr(String mappingStr) {

        if (Strings.isNullOrEmpty(mappingStr)) {
            return new HashSet<>();
        }

        Set<String> ret = new HashSet<>();
        String[] patientGuids = mappingStr.split(Pattern.quote(DELIM));
        for (String patientGuid: patientGuids) {
            ret.add(patientGuid);
        }
        return ret;
    }

    private static String getMappingStrFromPatientGuids(Set<String> patientGuids) {
        return String.join(DELIM, patientGuids);
    }

    /**
     * If we get an update to a slot, removing or changing the patient GUID (and not deleting the slot), then cancel any appt already booked in
     *
     * If we get a delete for a slot that still contains a patient GUID, then delete the appt
     * (implication is that the slot was deleted with the patient still booked, so this is an appt delete).
     *
     * If we get a delete for a slot, and there’s no patient GUID, then cancel any appts that were in the slot
     * (implication is that the patient appt was cancelled before the slot was booked).
     */
    static class SlotUpdateCallable implements Callable {

        private Map<CsvCell, CsvCell[]> batch;
        private EmisCsvHelper csvHelper;
        private FhirResourceFiler fhirResourceFiler;

        public SlotUpdateCallable(Map<CsvCell, CsvCell[]> batch, EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) {
            this.batch = batch;
            this.csvHelper = csvHelper;
            this.fhirResourceFiler = fhirResourceFiler;
        }

        @Override
        public Object call() throws Exception {
            try {

                Set<String> sourceIds = new HashSet<>();
                for (CsvCell slotGuidCell: batch.keySet()) {
                    String slotGuid = slotGuidCell.getString();
                    sourceIds.add(slotGuid);
                }

                //look up the previous patient GUID for the slots, and the full set of all patient GUIDs
                Map<String, String> hmPatientGuidMappings = internalIdDal.getDestinationIds(csvHelper.getServiceId(), SLOT_LATEST_PATIENT_GUID, sourceIds);

                //maintain a list of mappings that have changed
                List<InternalIdMap> mappingsToSave = new ArrayList<>();


                //for each slot GUID, see if the assigned patient GUID has changed
                for (CsvCell slotGuidCell: batch.keySet()) {
                    String slotGuid = slotGuidCell.getString();

                    CsvCell[] arr = batch.get(slotGuidCell);
                    CsvCell latestPatientGuidCell = arr[0];
                    CsvCell deletedCell = arr[1];

                    String latestPatientGuid = latestPatientGuidCell.getString(); //note this may be empty if the slot is now empty (i.e. hasn't been rebooked)
                    boolean isDeleted = deletedCell.getBoolean();

                    //compare the patient GUID in the cell currently against all previously associated patients and cancel anything for other patients
                    String allPreviousPatientsStr = hmPatientGuidMappings.get(slotGuid); //this may be empty if the slot was never previously booked
                    Set<String> previousPatientGuids = getPatientGuidsFromMappingStr(allPreviousPatientsStr);

                    //go through all known patient GUIDs associated with this cell and cancel or delete apps as necessary
                    for (String previousPatientGuid: previousPatientGuids) {

                        if (!previousPatientGuid.equals(latestPatientGuid)) {
                            //if the slot is assigned to a different patient now (or no patient at all), then cancel the appt
                            CsvCell previousPatientGuidCell = CsvCell.factoryDummyWrapper(previousPatientGuid);
                            cancelExistingAppointment(slotGuidCell, previousPatientGuidCell, latestPatientGuidCell, csvHelper, fhirResourceFiler);

                        } else if (isDeleted) {
                            //if the slot is still assigned to a patient and the slot is deleted, then delete that appt
                            deleteExistingAppointment(slotGuidCell, latestPatientGuidCell, deletedCell, csvHelper, fhirResourceFiler);
                        }
                    }

                    //make sure that the current patient GUID is in the mapping
                    if (!Strings.isNullOrEmpty(latestPatientGuid)
                        && !previousPatientGuids.contains(latestPatientGuid)) {

                        previousPatientGuids.add(latestPatientGuid);

                        String mappingStr = getMappingStrFromPatientGuids(previousPatientGuids);
                        InternalIdMap m = new InternalIdMap();
                        m.setServiceId(csvHelper.getServiceId());
                        m.setIdType(SLOT_LATEST_PATIENT_GUID);
                        m.setSourceId(slotGuid);
                        m.setDestinationId(mappingStr);
                        mappingsToSave.add(m);
                    }
                }

                //save any updated mappings
                if (!mappingsToSave.isEmpty()) {
                    internalIdDal.save(mappingsToSave);
                }

            } catch (Exception ex) {
                LOG.error("", ex);
                throw ex;
            }

            return null;
        }

    }

}
