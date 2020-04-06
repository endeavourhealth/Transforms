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
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

/**
 * detects if any previously received Slot records have had their appointments cancelled
 * and updates the FHIR Appoitment accordingly
 */
public class SlotPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SlotPreTransformer.class);

    private static final String SLOT_LATEST_PATIENT_GUID = "EmisSlotLatestPatientGuid";

    private static final InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        Map<CsvCell, CsvCell> batch = new HashMap<>();

        Slot parser =(Slot)parsers.get(Slot.class);
        while (parser != null && parser.nextRecord()) {

            try {
                if (csvHelper.shouldProcessRecord(parser)) {
                    processRecord(parser, fhirResourceFiler, csvHelper, batch);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        saveBatch(batch, true, csvHelper, fhirResourceFiler);
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void saveBatch(Map<CsvCell, CsvCell> batch, boolean lastOne, EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        csvHelper.submitToThreadPool(new BatchCallable(new HashMap<>(batch), csvHelper, fhirResourceFiler));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    public static void processRecord(Slot parser, FhirResourceFiler fhirResourceFiler, EmisCsvHelper csvHelper, Map<CsvCell, CsvCell> batch) throws Exception {
        CsvCell slotGuid = parser.getSlotGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        batch.put(slotGuid, patientGuid);
        saveBatch(batch, false, csvHelper, fhirResourceFiler);
    }

    static class BatchCallable implements Callable {

        private Map<CsvCell, CsvCell> batch;
        private EmisCsvHelper csvHelper;
        private FhirResourceFiler fhirResourceFiler;

        public BatchCallable(Map<CsvCell, CsvCell> batch, EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) {
            this.batch = batch;
            this.csvHelper = csvHelper;
            this.fhirResourceFiler = fhirResourceFiler;
        }

        @Override
        public Object call() throws Exception {
            try {

                //find the existing/previous patientGuid for each of our slots
                Set<String> sourceIds = new HashSet<>();
                for (CsvCell slotGuidCell: batch.keySet()) {
                    sourceIds.add(slotGuidCell.getString());
                }

                Map<String, String> existingPatientIds = internalIdDal.getDestinationIds(csvHelper.getServiceId(), SLOT_LATEST_PATIENT_GUID, sourceIds);

                for (CsvCell slotGuidCell: batch.keySet()) {
                    CsvCell newPatientGuidCell = batch.get(slotGuidCell);
                    String newPatientGuid = newPatientGuidCell.getString(); //note this may be empty if the slot is now empty (i.e. hasn't been rebooked)

                    String previousPatientGuid = existingPatientIds.get(slotGuidCell.getString());

                    if (!Strings.isNullOrEmpty(previousPatientGuid)
                            && !previousPatientGuid.equals(newPatientGuid)) {

                        cancelPreviousAppointment(slotGuidCell, newPatientGuidCell, previousPatientGuid, csvHelper, fhirResourceFiler);
                    }
                }

                //save the new patient guids
                List<InternalIdMap> newOnes = new ArrayList<>();
                for (CsvCell slotGuidCell: batch.keySet()) {
                    CsvCell newPatientGuidCell = batch.get(slotGuidCell);

                    InternalIdMap m = new InternalIdMap();
                    m.setServiceId(csvHelper.getServiceId());
                    m.setIdType(SLOT_LATEST_PATIENT_GUID);
                    m.setSourceId(slotGuidCell.getString());
                    m.setDestinationId(newPatientGuidCell.getString());
                    newOnes.add(m);
                }
                internalIdDal.save(newOnes);

            } catch (Exception ex) {
                LOG.error("", ex);
                throw ex;
            }

            return null;
        }

        /**
         * if we detect that the patientGUID has changed (or been cleared), this means the appointment was cancelled,
         * so we need to find the previous Appointment resource and mark it as such.
         */
        private static void cancelPreviousAppointment(CsvCell slotGuid, CsvCell newPatientGuidCell, String previousPatientGuid, EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

            CsvCell previousPatientGuidCell = CsvCell.factoryDummyWrapper(previousPatientGuid);
            String sourceId = EmisCsvHelper.createUniqueId(previousPatientGuidCell, slotGuid);
            Appointment appointment = (Appointment)csvHelper.retrieveResource(sourceId, ResourceType.Appointment);

            //if the appointment has already been deleted or is cancelled or DNA, then do nothing
            if (appointment == null
                    || appointment.getStatus() == Appointment.AppointmentStatus.CANCELLED
                    || appointment.getStatus() == Appointment.AppointmentStatus.NOSHOW) {
                return;
            }

            AppointmentBuilder appointmentBuilder = new AppointmentBuilder(appointment);
            appointmentBuilder.setStatus(Appointment.AppointmentStatus.CANCELLED, newPatientGuidCell);

            //save without mapping IDs as this has been retrieved from the DB
            fhirResourceFiler.savePatientResource(null, false, appointmentBuilder);
        }

    }
}
