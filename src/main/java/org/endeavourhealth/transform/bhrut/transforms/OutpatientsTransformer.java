package org.endeavourhealth.transform.bhrut.transforms;

import com.google.common.base.Strings;
import org.apache.commons.lang3.ObjectUtils;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.QuantityHelper;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.bhrut.schema.Outpatients;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import static org.endeavourhealth.common.ods.OdsWebService.lookupOrganisationViaRest;
import static org.endeavourhealth.transform.bhrut.BhrutCsvHelper.addParmIfNotNullNhsdd;


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
                if (!csvHelper.processRecordFilteringOnPatientId(parser)) {
                    continue;
                }
                try {
                    createResource((org.endeavourhealth.transform.bhrut.schema.Outpatients) parser, fhirResourceFiler, csvHelper);
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
                                      BhrutCsvHelper csvHelper) throws Exception {

        //Create the Encounter resource
        EncounterBuilder encounterBuilder = createEncounter(parser, csvHelper);

        AppointmentBuilder appointmentBuilder = new AppointmentBuilder();
        CsvCell idCell = parser.getId();
        String apptUniqueId = idCell.getString() + APPT_ID_SUFFIX;
        appointmentBuilder.setId(apptUniqueId, idCell);

        CsvCell patientIdCell = parser.getPasId();
        Reference newPatientReference = csvHelper.createPatientReference(patientIdCell);
        appointmentBuilder.addParticipant(newPatientReference, Appointment.ParticipationStatus.ACCEPTED, patientIdCell);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();
        if (dataUpdateStatusCell.getString().equalsIgnoreCase("Deleted")) {

            encounterBuilder.setDeletedAudit(dataUpdateStatusCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, encounterBuilder, appointmentBuilder);
            //then, delete the linked resources
            deleteClinicalResources(parser, fhirResourceFiler, csvHelper);
            return;
        }
        if (patientIdCell.isEmpty()) {
            TransformWarnings.log(LOG, csvHelper, "Missing patient id for {} ", idCell.getString());
            return;
        }

        //create the episode of care for the outpatient encounter
        createEpisodeOfCare(parser, fhirResourceFiler, csvHelper);

        //add the appointment ref to the encounter
        Reference appointmentRef = csvHelper.createAppointmentReference(apptUniqueId);
        encounterBuilder.setAppointment(appointmentRef);

        //the linked procedure and diagnosis records
        createProcedures(encounterBuilder, parser, fhirResourceFiler, csvHelper);
        createDiagnoses(encounterBuilder, parser, fhirResourceFiler, csvHelper);

        //the remaining appointment details
        CsvCell appointmentDateCell = parser.getAppointmentDttm();
        if (!appointmentDateCell.isEmpty()) {

            appointmentBuilder.setStartDateTime(appointmentDateCell.getDateTime(), appointmentDateCell);
        } else {
            LOG.debug("Appointment date empty for " + idCell.getString());
        }

        CsvCell endDateCell = parser.getApptDepartureDttm();
        if (!endDateCell.isEmpty()) {

            Date endDateTime = endDateCell.getDateTime();
            appointmentBuilder.setEndDateTime(endDateTime, endDateCell);
        }

        CsvCell apptTypeCell = parser.getApptType();
        if (!apptTypeCell.isEmpty()) {

            appointmentBuilder.setType(apptTypeCell.getString(), apptTypeCell);
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
        if (!endDateCell.isEmpty()) {
            Date dtSeen = patientSeenDateCell.getDateTime();
            Date appointmentDate = appointmentDateCell.getDateTime();
            Date callDate = parser.getApptCallDttm().getDateTime();

            Date appointmentStartDate
                    = ObjectUtils.firstNonNull(dtSeen, callDate, appointmentDate);
            Date dtLeft = endDateCell.getDateTime();
            long msDiff = dtLeft.getTime() - appointmentStartDate.getTime();
            long minDiff = msDiff / (1000L * 60L);

            appointmentBuilder.setMinutesActualDuration(new Integer((int) minDiff));
        }

        //this is just free text from the outcome
        CsvCell followUpDetails = parser.getAppointmentOutcome();
        if (!followUpDetails.isEmpty()) {
            appointmentBuilder.setComments(followUpDetails.getString(), followUpDetails);
        }

        //translate the appointment status
        if (!parser.getAppointmentStatusCode().isEmpty()) {

            CsvCell appointmentStatusCode = parser.getAppointmentStatusCode();
            translateAppointmentStatusCode(appointmentStatusCode, appointmentBuilder, csvHelper, idCell);
        }

        //add the encounter additional parameters
        ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(encounterBuilder);
        containedParametersBuilder.removeContainedParameters();

        CsvCell adminCategoryCodeCell = parser.getAdminCategoryCode();
        if (!adminCategoryCodeCell.isEmpty()) {

            addParmIfNotNullNhsdd("ADMIN_CATEGORY_CODE", adminCategoryCodeCell.getString(),
                    adminCategoryCodeCell, containedParametersBuilder, BhrutCsvToFhirTransformer.IM_OUTPATIENTS_TABLE_NAME);
        }
        CsvCell apptTypeCodeCell = parser.getApptTypeCode();
        if (!apptTypeCodeCell.isEmpty()) {

            addParmIfNotNullNhsdd("APPT_TYPE_CODE",
                    apptTypeCodeCell.getString(), apptTypeCodeCell,
                    containedParametersBuilder, BhrutCsvToFhirTransformer.IM_OUTPATIENTS_TABLE_NAME);
        }
        CsvCell appointmentStatusCodeCell = parser.getAppointmentStatusCode();
        if (!appointmentStatusCodeCell.isEmpty()) {

            addParmIfNotNullNhsdd("APPOINTMENT_STATUS_CODE", appointmentStatusCodeCell.getString(),
                    appointmentStatusCodeCell, containedParametersBuilder, BhrutCsvToFhirTransformer.IM_OUTPATIENTS_TABLE_NAME);
        }
        CsvCell appointmentOutcomeCodeCell = parser.getAppointmentOutcomeCode();
        if (!appointmentOutcomeCodeCell.isEmpty()) {

            addParmIfNotNullNhsdd(  "APPOINTMENT_OUTCOME_CODE",
                    appointmentOutcomeCodeCell.getString(), appointmentOutcomeCodeCell,
                    containedParametersBuilder, BhrutCsvToFhirTransformer.IM_OUTPATIENTS_TABLE_NAME);
        }
        //save the Encounter and Appointment
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), !encounterBuilder.isIdMapped(), encounterBuilder);
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), !appointmentBuilder.isIdMapped(), appointmentBuilder);
    }

    private static EncounterBuilder createEncounter(Outpatients parser, BhrutCsvHelper csvHelper) throws Exception {

        EncounterBuilder parentTopEncounterBuilder = new EncounterBuilder();
        parentTopEncounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);
        parentTopEncounterBuilder.setId(parser.getId().getString());

        //work out the Encounter status
        CsvCell outcomeCell = parser.getAppointmentStatusCode();
        if (!outcomeCell.isEmpty() && !outcomeCell.getString().equalsIgnoreCase("xxxx")) {
            //if we have an outcome, we know the encounter has ended
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED, outcomeCell);
        } else {
            //if we don't have an outcome, the Encounter is either in progress or in the future
            CsvCell beginDateCell = parser.getAppointmentDttm();
            Date beginDate = beginDateCell.getDateTime();
            if (beginDate == null
                    || beginDate.after(new Date())) {
                parentTopEncounterBuilder.setStatus(Encounter.EncounterState.PLANNED, beginDateCell);
            } else {
                parentTopEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS, beginDateCell);
            }
        }
        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Outpatient");

        setCommonEncounterAttributes(parentTopEncounterBuilder, parser, csvHelper, false);

        return parentTopEncounterBuilder;
    }

    private static void createDiagnoses(EncounterBuilder outpatientEncounterBuilder,
                                        Outpatients parser,
                                        FhirResourceFiler fhirResourceFiler,
                                        BhrutCsvHelper csvHelper) throws Exception {

        CsvCell idCell = parser.getId();
        CsvCell patientIdCell = parser.getPasId();
        CsvCell appointmentDateCell = parser.getAppointmentDttm();
        CsvCell consultantCodeCell = parser.getConsultantCode();

        //Primary Diagnosis
        CsvCell primaryDiagnosisCodeCell = parser.getPrimaryDiagnosisCode();
        if (!primaryDiagnosisCodeCell.isEmpty()) {
            ConditionBuilder conditionBuilder = new ConditionBuilder();
            conditionBuilder.setId(idCell.getString() + ":Condition:0");
            Reference patientReference2 = csvHelper.createPatientReference(patientIdCell);
            conditionBuilder.setPatient(patientReference2, patientIdCell);

            //create an Encounter reference for the conditions to use
            Reference thisEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, idCell.getString());
            if (outpatientEncounterBuilder.isIdMapped()) {
                thisEncounter = IdHelper.convertLocallyUniqueReferenceToEdsReference(thisEncounter, csvHelper);
            }
            conditionBuilder.setEncounter(thisEncounter, idCell);

            conditionBuilder.setAsProblem(false);
            conditionBuilder.setIsPrimary(true);
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
            String icd10 = TerminologyService.standardiseIcd10Code(primaryDiagnosisCodeCell.getString());
            if (icd10.endsWith("X")) {
                icd10 = icd10.substring(0, 3);
            } else if (icd10.length() > 4) {

            }
            codeableConceptBuilder.setCodingCode(icd10, primaryDiagnosisCodeCell);
            String diagTerm = TerminologyService.lookupIcd10CodeDescription(icd10);
            if (Strings.isNullOrEmpty(diagTerm)) {
                throw new Exception("Failed to find diagnosis term for ICD 10 code " + icd10);
            }
            codeableConceptBuilder.setCodingDisplay(diagTerm);
            Reference consultantReference5 = csvHelper.createPractitionerReference(consultantCodeCell.getString());
            conditionBuilder.setClinician(consultantReference5, consultantCodeCell);

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
                Reference diagPatientReference = csvHelper.createPatientReference(patientIdCell);
                conditionBuilder.setPatient(diagPatientReference, patientIdCell);

                //create an Encounter reference for the conditions to use
                Reference thisEncounter
                        = ReferenceHelper.createReference(ResourceType.Encounter, idCell.getString());
                if (outpatientEncounterBuilder.isIdMapped()) {
                    thisEncounter = IdHelper.convertLocallyUniqueReferenceToEdsReference(thisEncounter, csvHelper);
                }
                conditionBuilder.setEncounter(thisEncounter, idCell);

                conditionBuilder.setAsProblem(false);
                conditionBuilder.setIsPrimary(false);

                CodeableConceptBuilder codeableConceptBuilder
                        = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10);
                String icd10 = TerminologyService.standardiseIcd10Code(secondaryDiagnosisCodeCell.getString());
                if (icd10.endsWith("X")) {
                    icd10 = icd10.substring(0, 3);
                }
                codeableConceptBuilder.setCodingCode(icd10, secondaryDiagnosisCodeCell);
                String diagTerm = TerminologyService.lookupIcd10CodeDescription(icd10);
                if (Strings.isNullOrEmpty(diagTerm)) {
                    throw new Exception("Failed to find diagnosis term for ICD 10 code " + icd10);
                }
                codeableConceptBuilder.setCodingDisplay(diagTerm);
                Reference consultantReference6 = csvHelper.createPractitionerReference(consultantCodeCell.getString());
                conditionBuilder.setClinician(consultantReference6, consultantCodeCell);

                DateTimeType dateTimeType = new DateTimeType(appointmentDateCell.getDateTime());
                conditionBuilder.setOnset(dateTimeType, appointmentDateCell);
                conditionBuilder.setCategory("diagnosis");

                fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);
            } else {
                break;  //No point parsing empty cells. Assume non-empty cells are sequential.
            }
        }
    }

    private static void createProcedures(EncounterBuilder outpatientEncounterBuilder,
                                         Outpatients parser,
                                         FhirResourceFiler fhirResourceFiler,
                                         BhrutCsvHelper csvHelper) throws Exception {

        CsvCell idCell = parser.getId();
        CsvCell patientIdCell = parser.getPasId();
        CsvCell appointmentDateCell = parser.getAppointmentDttm();
        CsvCell consultantCodeCell = parser.getConsultantCode();
        //Primary Procedure
        CsvCell primaryProcedureCodeCell = parser.getPrimaryProcedureCode();
        if (!primaryProcedureCodeCell.isEmpty()) {

            ProcedureBuilder procedurePrimaryBuilder = new ProcedureBuilder();
            procedurePrimaryBuilder.setId(idCell.getString() + ":Procedure:0");
            Reference procPatientReference = csvHelper.createPatientReference(patientIdCell);
            procedurePrimaryBuilder.setPatient(procPatientReference, patientIdCell);

            Reference procEncReference
                    = ReferenceHelper.createReference(ResourceType.Encounter, idCell.getString());
            if (outpatientEncounterBuilder.isIdMapped()) {
                procEncReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(procEncReference, csvHelper);
            }
            procedurePrimaryBuilder.setEncounter(procEncReference, idCell);
            procedurePrimaryBuilder.setIsPrimary(true);

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(procedurePrimaryBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);
            codeableConceptBuilder.setCodingCode(primaryProcedureCodeCell.getString(), primaryProcedureCodeCell);
            String procTerm = TerminologyService.lookupOpcs4ProcedureName(parser.getPrimaryProcedureCode().getString());
            if (Strings.isNullOrEmpty(procTerm)) {
                throw new Exception("Failed to find procedure term for OPCS-4 code " + parser.getPrimaryProcedureCode().getString());
            }
            codeableConceptBuilder.setCodingDisplay(procTerm); //don't pass in a cell as this was derived
            Reference consultantReference3 = csvHelper.createPractitionerReference(consultantCodeCell.getString());
            procedurePrimaryBuilder.addPerformer(consultantReference3, consultantCodeCell);

            DateTimeType dateTimeType = new DateTimeType(appointmentDateCell.getDateTime());
            procedurePrimaryBuilder.setPerformed(dateTimeType, appointmentDateCell);
            DateTimeType endedDateTimeType = new DateTimeType(parser.getApptDepartureDttm().getDateTime());
            procedurePrimaryBuilder.setEnded(endedDateTimeType,parser.getApptDepartureDttm());

            fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedurePrimaryBuilder);
        }

        //Secondary Procedure(s)
        for (int i = 1; i <= 11; i++) {
            Method method = Outpatients.class.getDeclaredMethod("getSecondaryProcedureCode" + i);
            CsvCell secondaryProcedureCodeCell = (CsvCell) method.invoke(parser);
            if (!secondaryProcedureCodeCell.isEmpty()) {

                ProcedureBuilder procedureBuilder = new ProcedureBuilder();
                procedureBuilder.setId(idCell.getString() + ":Procedure:" + i);
                Reference patientReference2 = csvHelper.createPatientReference(patientIdCell);
                procedureBuilder.setPatient(patientReference2, patientIdCell);

                Reference procEncReference
                        = ReferenceHelper.createReference(ResourceType.Encounter, idCell.getString());
                if (outpatientEncounterBuilder.isIdMapped()) {
                    procEncReference
                            = IdHelper.convertLocallyUniqueReferenceToEdsReference(procEncReference, csvHelper);
                }
                procedureBuilder.setEncounter(procEncReference, idCell);
                procedureBuilder.setIsPrimary(false);

                CodeableConceptBuilder codeableConceptBuilder
                        = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);
                codeableConceptBuilder.setCodingCode(secondaryProcedureCodeCell.getString(), secondaryProcedureCodeCell);
                String procTerm = TerminologyService.lookupOpcs4ProcedureName(secondaryProcedureCodeCell.getString());
                if (Strings.isNullOrEmpty(procTerm)) {
                    throw new Exception("Failed to find procedure term for OPCS-4 code " + parser.getPrimaryProcedureCode().getString());
                }
                codeableConceptBuilder.setCodingDisplay(procTerm);
                Reference consultantReference4 = csvHelper.createPractitionerReference(consultantCodeCell.getString());
                procedureBuilder.addPerformer(consultantReference4, consultantCodeCell);

                DateTimeType dateTimeType = new DateTimeType(appointmentDateCell.getDateTime());
                procedureBuilder.setPerformed(dateTimeType, appointmentDateCell);

                DateTimeType endedDateTimeType = new DateTimeType(parser.getApptDepartureDttm().getDateTime());
                procedureBuilder.setEnded(endedDateTimeType,parser.getApptDepartureDttm());

                fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
            } else {
                break;  //No point parsing any further empty cells.
            }
        }
    }

    private static void setCommonEncounterAttributes(EncounterBuilder builder,
                                                     Outpatients parser,
                                                     BhrutCsvHelper csvHelper,
                                                     boolean isChildEncounter) throws Exception {

        //every encounter has the following common attributes
        CsvCell patientIdCell = parser.getPasId();
        if (!patientIdCell.isEmpty()) {

            Reference patientReference
                    = ReferenceHelper.createReference(ResourceType.Patient, patientIdCell.getString());
            if (builder.isIdMapped()) {
                patientReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, csvHelper);
            }
            builder.setPatient(patientReference);
        }

        //encounter start and end date/times set as the appointment seen and departure date/times
        if (!parser.getApptSeenDttm().isEmpty()) {
            builder.setPeriodStart(parser.getApptSeenDttm().getDateTime(), parser.getApptSeenDttm());
        }
        if (!parser.getApptDepartureDttm().isEmpty()) {
            builder.setPeriodEnd(parser.getApptDepartureDttm().getDateTime(),parser.getApptDepartureDttm());
        }

        CsvCell idCell = parser.getId();
        if (!idCell.isEmpty()) {
            Reference episodeReference
                    = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, idCell.getString());
            if (builder.isIdMapped()) {
                episodeReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeReference, csvHelper);
            }
            builder.setEpisodeOfCare(episodeReference);
        }

        CsvCell admissionConsultantCodeCell = parser.getConsultantCode();
        if (!admissionConsultantCodeCell.isEmpty()) {

            Reference practitionerReference
                    = ReferenceHelper.createReference(ResourceType.Practitioner, admissionConsultantCodeCell.getString());
            if (builder.isIdMapped()) {
                practitionerReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }
            builder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER);
        }

        CsvCell hospitalCodeCell = parser.getHospitalCode();
        if (!hospitalCodeCell.isEmpty()) {

            // Test if the hospital code is a local code that we can map to ODS
            String hospitalCode = null;
            if (lookupOrganisationViaRest(hospitalCodeCell.getString()) == null) {
                hospitalCode = hospitalCodeCell.getString();
            } else {
                hospitalCode = BhrutCsvToFhirTransformer.BHRUT_ORG_ODS_CODE;
            }
            Reference providerReference = csvHelper.createOrganisationReference(hospitalCode);
            if (builder.isIdMapped()) {
                providerReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(providerReference, csvHelper);
            }
            builder.setServiceProvider(providerReference);
        }

        if (isChildEncounter) {
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, idCell.getString());

            parentEncounter
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, csvHelper);

            builder.setPartOf(parentEncounter);
        }
    }

    private static void deleteClinicalResources(Outpatients parser,
                                                FhirResourceFiler fhirResourceFiler,
                                                BhrutCsvHelper csvHelper) throws Exception {
        CsvCell idCell = parser.getId();
        CsvCell dataUpdateStatusCell = parser.getDataUpdateStatus();
        CsvCell patientIdCell = parser.getPasId();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);

        //delete primary diagnosis and secondaries
        if (!parser.getPrimaryDiagnosisCode().isEmpty()) {
            ConditionBuilder condition = new ConditionBuilder();
            condition.setId(idCell.getString() + "Condition:0");
            condition.setPatient(patientReference, patientIdCell);
            condition.setDeletedAudit(dataUpdateStatusCell);
            condition.setAsProblem(false);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, condition);

            for (int i = 1; i <= 11; i++) {
                Method method = Outpatients.class.getDeclaredMethod("getSecondaryDiagnosisCode" + i);
                CsvCell diagCode = (CsvCell) method.invoke(parser);
                if (!diagCode.isEmpty()) {
                    ConditionBuilder conditionBuilder = new ConditionBuilder();
                    conditionBuilder.setId(idCell.getString() + "Condition:" + i);
                    conditionBuilder.setPatient(patientReference, patientIdCell);
                    conditionBuilder.setDeletedAudit(dataUpdateStatusCell);
                    conditionBuilder.setAsProblem(false);

                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, conditionBuilder);
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
            proc.setDeletedAudit(dataUpdateStatusCell);

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, proc);

            for (int i = 1; i <= 3; i++) {
                Method method = Outpatients.class.getDeclaredMethod("getSecondaryProcedureCode" + i);
                CsvCell procCode = (CsvCell) method.invoke(parser);
                if (!procCode.isEmpty()) {
                    ProcedureBuilder procedureBuilder = new ProcedureBuilder();
                    procedureBuilder.setId(idCell.getString() + ":Procedure:" + i);
                    procedureBuilder.setPatient(patientReference, patientIdCell);
                    procedureBuilder.setDeletedAudit(dataUpdateStatusCell);

                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, procedureBuilder);
                } else {
                    break;  //No point parsing empty cells. Assume non-empty cells are sequential.
                }
            }
        }
    }

    private static void createEpisodeOfCare(Outpatients parser,
                                            FhirResourceFiler fhirResourceFiler,
                                            BhrutCsvHelper csvHelper) throws Exception {


        CsvCell idCell = parser.getId();
        EpisodeOfCareBuilder episodeBuilder
                = csvHelper.getEpisodeOfCareCache().getOrCreateEpisodeOfCareBuilder(idCell, csvHelper, fhirResourceFiler);

        CsvCell patientIdCell = parser.getPasId();
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
        if (!odsCodeCell.isEmpty()) {
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
        if (!consultantCodeCell.isEmpty()) {
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

        csvHelper.getEpisodeOfCareCache().cacheEpisodeOfCareBuilder(idCell, episodeBuilder);
        fhirResourceFiler.savePatientResource(parser.getCurrentState(),!episodeBuilder.isIdMapped(),episodeBuilder);
    }
    private static void translateAppointmentStatusCode(CsvCell appointmentStatusCode, AppointmentBuilder appointmentBuilder, BhrutCsvHelper csvHelper,CsvCell idCell) throws Exception {
        // from the NHS data dictionary ATTENDED OR DID NOT ATTEND
        // 5	Attended on time or, if late, before the relevant CARE PROFESSIONAL was ready to see the PATIENT
        // 6	Arrived late, after the relevant CARE PROFESSIONAL was ready to see the PATIENT, but was seen
        // 7	PATIENT arrived late and could not be seen
        // 2	APPOINTMENT cancelled by, or on behalf of, the PATIENT
        // 3	Did not attend - no advance warning given
        // 4	APPOINTMENT cancelled or postponed by the Health Care Provider
        // 0   Not applicable - APPOINTMENT occurs in the future
        if (!appointmentStatusCode.isEmpty()) {
            if (appointmentStatusCode.getString().toLowerCase().contains("x")) {
                appointmentBuilder.setStatus(Appointment.AppointmentStatus.NULL);
                return;} //Indicates missing data
            try {
                int statusCode = Integer.parseInt(appointmentStatusCode.getString());
                switch (statusCode) { //Ostensibly an int but there's garbage in the field
                    case 2:
                    case 4:
                        appointmentBuilder.setStatus(Appointment.AppointmentStatus.CANCELLED);
                        break;
                    case 3:
                        appointmentBuilder.setStatus(Appointment.AppointmentStatus.NOSHOW);
                        break;
                    case 5:
                        appointmentBuilder.setStatus(Appointment.AppointmentStatus.FULFILLED);
                        break;
                    case 6:
                        appointmentBuilder.setStatus(Appointment.AppointmentStatus.FULFILLED);
                        break;
                    case 7:
                        appointmentBuilder.setStatus(Appointment.AppointmentStatus.NOSHOW);
                        break;
                    case 0:
                        appointmentBuilder.setStatus(Appointment.AppointmentStatus.PENDING);
                        break;
                    default:
                        TransformWarnings.log(LOG, csvHelper, "Unknown appointment status code integer {} for id {} ", statusCode, idCell.getString());
                }
            } catch (NumberFormatException ex) {
                TransformWarnings.log(LOG, csvHelper, "Invalid appointment status code - not integer {} ", appointmentStatusCode.getString());
            }
        }
    }
}