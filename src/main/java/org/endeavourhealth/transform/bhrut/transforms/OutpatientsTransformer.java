package org.endeavourhealth.transform.bhrut.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.QuantityHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.Outpatients;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class OutpatientsTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(OutpatientsTransformer.class);
    public static final String APPT_ID_SUFFIX = ":Appointment";

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(org.endeavourhealth.transform.bhrut.schema.Outpatients.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((org.endeavourhealth.transform.bhrut.schema.Outpatients) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(org.endeavourhealth.transform.bhrut.schema.Outpatients parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BhrutCsvHelper csvHelper,
                                      String version) throws Exception {

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        CsvCell idCell = parser.getId();
        encounterBuilder.setId(idCell.getString());

        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        encounterBuilder.setPatient(patientReference, patientIdCell);

        AppointmentBuilder appointmentBuilder = new AppointmentBuilder();
        String apptUniqueId = idCell.getString() + APPT_ID_SUFFIX;
        appointmentBuilder.setId(apptUniqueId, idCell);
        SlotBuilder slotBuilder = new SlotBuilder();
        slotBuilder.setId(apptUniqueId, idCell);
        appointmentBuilder.addParticipant(patientReference, Appointment.ParticipationStatus.ACCEPTED, patientIdCell);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell actionCell = parser.getLinestatus();
        if (actionCell.getString().equalsIgnoreCase("Delete")) {
            encounterBuilder.setDeletedAudit(actionCell);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder, slotBuilder, appointmentBuilder);

            //then, delete the linked resources
            deleteChildResources(parser, fhirResourceFiler, csvHelper, version);
            return;
        }

        //add the slot reference to the appointment
        Reference slotRef = csvHelper.createSlotReference(apptUniqueId);
        appointmentBuilder.addSlot(slotRef);

        //add the appointment ref to the encounter
        Reference appointmentRef = csvHelper.createAppointmentReference(apptUniqueId);
        encounterBuilder.setAppointment(appointmentRef);

        //use the appointment date as the clinical date for the linked diagnosis / procedures as this
        //date and time is always present in the record
        CsvCell appointmentDateCell = parser.getAppointmentDttm();
        if (!appointmentDateCell.isEmpty()) {
            slotBuilder.setStartDateTime(appointmentDateCell.getDate(), appointmentDateCell);
            appointmentBuilder.setStartDateTime(appointmentDateCell.getDate(), appointmentDateCell);
        }

        //the class is Outpatient
        encounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);

        CsvCell consultantCodeCell = parser.getConsultantCode();
        Reference consultantReference = csvHelper.createPractitionerReference(consultantCodeCell.getString());
        encounterBuilder.addParticipant(consultantReference, EncounterParticipantType.CONSULTANT, consultantCodeCell);

        //TODO - work out episode of care creation for BHRUT
        //link the consultation to our episode of care
        //Reference episodeReference = csvHelper.createEpisodeReference(patientIdCell);
        //encounterBuilder.setEpisodeOfCare(episodeReference);
        createEpisodeOfcare(parser, fhirResourceFiler, csvHelper, version);

        //we have no status field in the source data, but will only receive completed encounters, so we can infer this
        encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

        //set the service provider reference from the hospital ods code
        CsvCell hospitalOdsCodeCell = parser.getHospitalCode();
        Reference organisationReference = csvHelper.createOrganisationReference(hospitalOdsCodeCell.getString());
        if (encounterBuilder.isIdMapped()) {
            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, csvHelper);
        }
        encounterBuilder.setServiceProvider(organisationReference);

        //create an Encounter reference for the procedures and conditions to use
        Reference thisEncounter = csvHelper.createEncounterReference(idCell.getString(), patientIdCell.getString());

        //Primary Procedure
        CsvCell primaryProcedureCodeCell = parser.getPrimaryProcedureCode();
        if (!primaryProcedureCodeCell.isEmpty()) {

            ProcedureBuilder procedureBuilder = new ProcedureBuilder();
            procedureBuilder.setId(idCell.getString() + ":Procedure:0");
            procedureBuilder.setPatient(patientReference, patientIdCell);
            procedureBuilder.setEncounter(thisEncounter, idCell);
            procedureBuilder.setIsPrimary(true);
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);
            codeableConceptBuilder.setCodingCode(primaryProcedureCodeCell.getString(), primaryProcedureCodeCell);
            String procTerm = TerminologyService.lookupOpcs4ProcedureName(parser.getPrimaryProcedureCode().getString());
            if (Strings.isNullOrEmpty(procTerm)) {
                throw new Exception("Failed to find procedure term for OPCS-4 code " + parser.getPrimaryProcedureCode().getString());
            }
            codeableConceptBuilder.setCodingDisplay(procTerm); //don't pass in a cell as this was derived
            procedureBuilder.addPerformer(consultantReference, consultantCodeCell);

            DateTimeType dateTimeType = new DateTimeType(appointmentDateCell.getDateTime());
            procedureBuilder.setPerformed(dateTimeType, appointmentDateCell);

            fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
        }

        //Secondary Procedure(s)
        for (int i = 1; i <= 11; i++) {
            Method method = Outpatients.class.getDeclaredMethod("getSecondaryProcedureCode" + i);
            CsvCell secondaryProcedureCodeCell = (CsvCell) method.invoke(parser);
            if (!secondaryProcedureCodeCell.isEmpty()) {

                ProcedureBuilder procedureBuilder = new ProcedureBuilder();
                procedureBuilder.setId(idCell.getString() + ":Procedure:" + i);
                procedureBuilder.setPatient(patientReference, patientIdCell);
                procedureBuilder.setEncounter(thisEncounter, idCell);
                procedureBuilder.setIsPrimary(false);
                CodeableConceptBuilder codeableConceptBuilder
                        = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);
                codeableConceptBuilder.setCodingCode(secondaryProcedureCodeCell.getString(), secondaryProcedureCodeCell);
                String procTerm = TerminologyService.lookupOpcs4ProcedureName(secondaryProcedureCodeCell.getString());
                if (Strings.isNullOrEmpty(procTerm)) {
                    throw new Exception("Failed to find procedure term for OPCS-4 code " + parser.getPrimaryProcedureCode().getString());
                }
                procedureBuilder.addPerformer(consultantReference, consultantCodeCell);

                DateTimeType dateTimeType = new DateTimeType(appointmentDateCell.getDateTime());
                procedureBuilder.setPerformed(dateTimeType, appointmentDateCell);

                fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
            } else {
                break;  //No point parsing any further empty cells.
            }
        }

        //Primary Diagnosis
        CsvCell primaryDiagnosisCodeCell = parser.getPrimaryDiagnosisCode();
        if (!primaryDiagnosisCodeCell.isEmpty()) {
            ConditionBuilder conditionBuilder = new ConditionBuilder();
            conditionBuilder.setId(idCell.getString() + ":Condition:0");
            conditionBuilder.setPatient(patientReference, patientIdCell);
            conditionBuilder.setEncounter(thisEncounter, idCell);
            conditionBuilder.setAsProblem(false);
            conditionBuilder.setIsPrimary(true);
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
            codeableConceptBuilder.setCodingCode(primaryDiagnosisCodeCell.getString(), primaryDiagnosisCodeCell);
            String diagTerm = TerminologyService.lookupIcd10CodeDescription(parser.getPrimaryDiagnosisCode().getString());
            if (Strings.isNullOrEmpty(diagTerm)) {
                throw new Exception("Failed to find diagnosis term for ICD 10 code " + parser.getPrimaryDiagnosisCode().getString());
            }
            codeableConceptBuilder.setCodingDisplay(diagTerm);

            conditionBuilder.setClinician(consultantReference, consultantCodeCell);

            DateTimeType dateTimeType = new DateTimeType(appointmentDateCell.getDateTime());
            conditionBuilder.setOnset(dateTimeType, appointmentDateCell);
            conditionBuilder.setCategory("diagnosis");

            fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);
        }

        //Secondary Diagnosis(s)
        for (int i = 1; i <= 3; i++) {
            Method method = Outpatients.class.getDeclaredMethod("getSecondaryDiagnosisCode" + i);
            CsvCell secondaryDiagnosisCodeCell = (CsvCell) method.invoke(parser);
            if (!secondaryDiagnosisCodeCell.isEmpty()) {

                ConditionBuilder conditionBuilder = new ConditionBuilder();
                conditionBuilder.setId(idCell.getString() + ":Condition:" + i);
                conditionBuilder.setPatient(patientReference, patientIdCell);
                conditionBuilder.setEncounter(thisEncounter, idCell);
                conditionBuilder.setAsProblem(false);
                conditionBuilder.setIsPrimary(false);

                CodeableConceptBuilder codeableConceptBuilder
                        = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
                codeableConceptBuilder.setCodingCode(secondaryDiagnosisCodeCell.getString(), secondaryDiagnosisCodeCell);
                String diagTerm = TerminologyService.lookupIcd10CodeDescription(secondaryDiagnosisCodeCell.getString());
                if (Strings.isNullOrEmpty(diagTerm)) {
                    throw new Exception("Failed to find diagnosis term for ICD 10 code " + parser.getPrimaryDiagnosisCode().getString());
                }
                codeableConceptBuilder.setCodingDisplay(diagTerm);
                conditionBuilder.setClinician(consultantReference, consultantCodeCell);

                DateTimeType dateTimeType = new DateTimeType(appointmentDateCell.getDateTime());
                conditionBuilder.setOnset(dateTimeType, appointmentDateCell);
                conditionBuilder.setCategory("diagnosis");

                fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);
            } else {
                break;  //No point parsing empty cells. Assume non-empty cells are sequential.
            }
        }

        CsvCell endDateCell = parser.getApptDepartureDttm();
        if (!endDateCell.isEmpty()) {
            Date endDateTime = endDateCell.getDateTime();
            slotBuilder.setEndDateTime(endDateTime, endDateCell);
            appointmentBuilder.setEndDateTime(endDateTime, endDateCell);
        }

        CsvCell telephoneApptCell = parser.getApptType();
        if (telephoneApptCell.getBoolean()) {
            appointmentBuilder.setType(telephoneApptCell.getString(), telephoneApptCell);
        }

        CsvCell bookingDateCell = parser.getBookedDttm();
        if (!bookingDateCell.isEmpty()) {
            Date d = bookingDateCell.getDateTime();
            appointmentBuilder.setBookedDateTime(d, bookingDateCell);
        }

        CsvCell cancelledDateCell = parser.getCancelDttm();
        if (!cancelledDateCell.isEmpty()) {
            Date d = cancelledDateCell.getDateTime();
            appointmentBuilder.setCancelledDateTime(d, cancelledDateCell);
        }

        CsvCell patientSeenDateCell = parser.getApptSeenDttm();
        if (!patientSeenDateCell.isEmpty()) {
            Date d = patientSeenDateCell.getDateTime();
            appointmentBuilder.setSentInDateTime(d, patientSeenDateCell);
        }

        //calculate the delay as the difference between the scheduled start and actual start
        if (!appointmentDateCell.isEmpty()
                && !patientSeenDateCell.isEmpty()) {
            Date dtScheduledStart = appointmentDateCell.getDateTime();
            Date dtSeen = patientSeenDateCell.getDateTime();
            long msDiff = dtSeen.getTime() - dtScheduledStart.getTime();
            if (msDiff >= 0) { //if patient was seen early, then there's no delay
                long minDiff = msDiff / (1000L * 60L);
                Duration fhirDuration = QuantityHelper.createDuration(new Integer((int) minDiff), "minutes");
                appointmentBuilder.setPatientDelay(fhirDuration, appointmentDateCell, patientSeenDateCell);
            }
        }

        //calculate the total patient wait as the difference between the arrival time and when they were seen
        CsvCell dtArrivedCell = parser.getApptArrivalDttm();
        if (!dtArrivedCell.isEmpty()
                && !patientSeenDateCell.isEmpty()) {

            Date dtArrived = dtArrivedCell.getDateTime();
            Date dtSeen = patientSeenDateCell.getDateTime();
            long msDiff = dtSeen.getTime() - dtArrived.getTime();
            long minDiff = msDiff / (1000L * 60L);
            Duration fhirDuration = QuantityHelper.createDuration(new Integer((int) minDiff), "minutes");
            appointmentBuilder.setPatientWait(fhirDuration, dtArrivedCell, patientSeenDateCell);
        }

        //calculate the total duration as the difference between the seen time and when they left
        if (!patientSeenDateCell.isEmpty() && !endDateCell.isEmpty()) {

            Date dtSeen = patientSeenDateCell.getDateTime();
            Date dtLeft = endDateCell.getDateTime();
            long msDiff = dtLeft.getTime() - dtSeen.getTime();
            long minDiff = msDiff / (1000L * 60L);

            appointmentBuilder.setMinutesActualDuration(new Integer((int) minDiff));
        }

        // from the NHS data dictionary ATTENDED OR DID NOT ATTEND
        // 5	Attended on time or, if late, before the relevant CARE PROFESSIONAL was ready to see the PATIENT
        // 6	Arrived late, after the relevant CARE PROFESSIONAL was ready to see the PATIENT, but was seen
        // 7	PATIENT arrived late and could not be seen
        // 2	APPOINTMENT cancelled by, or on behalf of, the PATIENT
        // 3	Did not attend - no advance warning given
        // 4	APPOINTMENT cancelled or postponed by the Health Care Provider
        // 0   Not applicable - APPOINTMENT occurs in the future
        CsvCell appointmentStatusCode = parser.getAppointmentStatusCode();
        if (!appointmentStatusCode.isEmpty()) {

            switch (appointmentStatusCode.getString()) {
                case "2":
                    appointmentBuilder.setStatus(Appointment.AppointmentStatus.CANCELLED);
                case "3":
                    appointmentBuilder.setStatus(Appointment.AppointmentStatus.NOSHOW);
                case "4":
                    appointmentBuilder.setStatus(Appointment.AppointmentStatus.CANCELLED);
                case "5":
                    appointmentBuilder.setStatus(Appointment.AppointmentStatus.FULFILLED);
                case "6":
                    appointmentBuilder.setStatus(Appointment.AppointmentStatus.FULFILLED);
                case "7":
                    appointmentBuilder.setStatus(Appointment.AppointmentStatus.NOSHOW);
                case "0":
                    appointmentBuilder.setStatus(Appointment.AppointmentStatus.PENDING);
            }
        }

        //this is just free text from the outcome
        CsvCell followUpDetails = parser.getAppointmentOutcome();
        if (!followUpDetails.isEmpty()) {
            appointmentBuilder.setComments(followUpDetails.getString(), followUpDetails);
        }

        //add the three Encounter extensions
        if (!parser.getAdminCategoryCode().isEmpty()) {
            CsvCell adminCategoryCode = parser.getAdminCategoryCode();
            CsvCell adminCategory = parser.getAdminCategory();
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Admin_Category);
            cc.setText(adminCategory.getString(), adminCategory);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(adminCategoryCode.getString(), adminCategoryCode);
            cc.setCodingDisplay(adminCategory.getString(), adminCategory);
        }
        if (!appointmentStatusCode.isEmpty()) {
            CsvCell appointmentStatus = parser.getAppointmentStatus();
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Appointment_Attended);
            cc.setText(appointmentStatus.getString(), appointmentStatus);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(appointmentStatusCode.getString(), appointmentStatusCode);
            cc.setCodingDisplay(appointmentStatus.getString(), appointmentStatus);
        }
        if (!parser.getAppointmentOutcomeCode().isEmpty()) {
            CsvCell appointmentOutcomeCode = parser.getAppointmentOutcomeCode();
            CsvCell appointmentOutcome = parser.getAppointmentOutcome();
            CodeableConceptBuilder cc
                    = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Appointment_Outcome);
            cc.setText(appointmentOutcome.getString(), appointmentOutcome);
            cc.addCoding(FhirCodeUri.CODE_SYSTEM_NHS_DD);
            cc.setCodingCode(appointmentOutcomeCode.getString(), appointmentOutcomeCode);
            cc.setCodingDisplay(appointmentOutcome.getString(), appointmentOutcome);
        }

        //save the Encounter, Appointment and Slot
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder, appointmentBuilder, slotBuilder);
    }

    private static void deleteChildResources(Outpatients parser,
                                             FhirResourceFiler fhirResourceFiler,
                                             BhrutCsvHelper csvHelper,
                                             String version) throws Exception {
        CsvCell idCell = parser.getId();
        CsvCell actionCell = parser.getLinestatus();
        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);

        //delete primary diagnosis and secondaries
        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {
            ConditionBuilder condition = new ConditionBuilder();
            condition.setId(idCell.getString() + "Condition:0");
            condition.setPatient(patientReference, patientIdCell);
            condition.setDeletedAudit(actionCell);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), condition);

            for (int i = 1; i <= 11; i++) {
                Method method = Outpatients.class.getDeclaredMethod("getSecondaryDiagnosisCode" + i);
                CsvCell diagCode = (CsvCell) method.invoke(parser);
                if (!diagCode.isEmpty()) {
                    ConditionBuilder conditionBuilder = new ConditionBuilder();
                    conditionBuilder.setId(idCell.getString() + "Condition:" + i);
                    conditionBuilder.setPatient(patientReference, patientIdCell);
                    conditionBuilder.setDeletedAudit(actionCell);

                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), conditionBuilder);
                } else {
                    break;  //No point parsing empty cells. Assume non-empty cells are sequential.
                }
            }
        }
        //delete primary procedures and secondaries
        if (!parser.getPrimaryProcedureCode().isEmpty()) {
            ProcedureBuilder proc = new ProcedureBuilder();
            proc.setId(idCell.getString() + ":Procedure:0", idCell);
            proc.setPatient(patientReference, patientIdCell);
            proc.setDeletedAudit(actionCell);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), proc);

            for (int i = 1; i <= 3; i++) {
                Method method = Outpatients.class.getDeclaredMethod("getSecondaryProcedureCode" + i);
                CsvCell procCode = (CsvCell) method.invoke(parser);
                if (!procCode.isEmpty()) {
                    ProcedureBuilder procedureBuilder = new ProcedureBuilder();
                    procedureBuilder.setId(idCell.getString() + ":Procedure:" + i);
                    procedureBuilder.setPatient(patientReference, patientIdCell);
                    procedureBuilder.setDeletedAudit(actionCell);

                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
                } else {
                    break;  //No point parsing empty cells. Assume non-empty cells are sequential.
                }
            }
        }
    }

    private static void createEpisodeOfcare(Outpatients parser, FhirResourceFiler fhirResourceFiler, BhrutCsvHelper csvHelper, String version) throws Exception {

        CsvCell patientIdCell = parser.getPasId();
        CsvCell id = parser.getId();

        EpisodeOfCareBuilder episodeBuilder
                = csvHelper.getEpisodeOfCareCache().getOrCreateEpisodeOfCareBuilder(patientIdCell, csvHelper, fhirResourceFiler);

        Reference patientReference = csvHelper.createPatientReference(patientIdCell);

        if (episodeBuilder.isIdMapped()) {
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
        }
        episodeBuilder.setPatient(patientReference, patientIdCell);

        CsvCell startDateTime = parser.getApptSeenDttm();
        if (!startDateTime.isEmpty()) {
            episodeBuilder.setRegistrationStartDate(startDateTime.getDateTime(), startDateTime);
        }
        CsvCell endDateTime = parser.getApptDepartureDttm();
        if (!endDateTime.isEmpty()) {
            episodeBuilder.setRegistrationEndDate(endDateTime.getDateTime(), endDateTime);
        }
        CsvCell odsCodeCell = parser.getHospitalCode();
        if (odsCodeCell != null) {
            Reference organisationReference = csvHelper.createOrganisationReference(odsCodeCell.getString());
            // if episode already ID mapped, get the mapped ID for the org
            if (episodeBuilder.isIdMapped()) {
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
            }
            episodeBuilder.setManagingOrganisation(organisationReference, odsCodeCell);
        } else {
            //v1 uses service details
            UUID serviceId = parser.getServiceId();
            Reference organisationReference = csvHelper.createOrganisationReference(serviceId.toString());
            // if episode already ID mapped, get the mapped ID for the org
            if (episodeBuilder.isIdMapped()) {
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
            }
            episodeBuilder.setManagingOrganisation(organisationReference);
        }

        CsvCell consultantCodeCell = parser.getConsultantCode();
        if (consultantCodeCell != null && !consultantCodeCell.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReference(consultantCodeCell.getString());
            if (episodeBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, fhirResourceFiler);
            }
            episodeBuilder.setCareManager(practitionerReference, consultantCodeCell);
        }

        //simple priority text set as an extension
        CsvCell priority = parser.getAppointmentPriority();
        if (!priority.isEmpty()) {
            episodeBuilder.setPriority(priority.getString(), priority);
        }

        csvHelper.getEpisodeOfCareCache().returnEpisodeOfCareBuilder(id, episodeBuilder);
    }

}