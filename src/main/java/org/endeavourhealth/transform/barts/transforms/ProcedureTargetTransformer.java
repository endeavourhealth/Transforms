package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedureTarget;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Procedure;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.endeavourhealth.transform.barts.CodeValueSet.SURGEON_SPECIALITY_GROUP;

public class ProcedureTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ProcedureTargetTransformer.class);

    public static void transform(FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            createProcedures(fhirResourceFiler, csvHelper);
        } catch (Exception ex) {
            fhirResourceFiler.logTransformRecordError(ex, null);
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    private static void createProcedures(FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // retrieve the target procedures for the current exchangeId
        List<StagingProcedureTarget> targetProcedures = csvHelper.retrieveTargetProcedures();
        if (targetProcedures == null) {
            return;
        }

        //TransformWarnings.log(LOG, csvHelper, "Target Procedures to transform to FHIR: {} for exchangeId: {}", targetProcedures.size(), csvHelper.getExchangeId());

        for (StagingProcedureTarget targetProcedure : targetProcedures) {

            String uniqueId = targetProcedure.getUniqueId();
            boolean isDeleted = targetProcedure.isDeleted();

            if (isDeleted) {

                // retrieve the existing Procedure resource to perform the deletion on
                Procedure existingProcedure = (Procedure) csvHelper.retrieveResourceForLocalId(ResourceType.Procedure, uniqueId);

                if (existingProcedure != null) {
                    ProcedureBuilder procedureBuilder = new ProcedureBuilder(existingProcedure, targetProcedure.getAudit());

                    //remember to pass in false since this existing procedure is already ID mapped
                    fhirResourceFiler.deletePatientResource(null, false, procedureBuilder);
                } else {
                    TransformWarnings.log(LOG, csvHelper, "Cannot find existing Procedure: {} for deletion", uniqueId);
                }

                continue;
            }

            // create the FHIR Procedure resource - NOTE
            ProcedureBuilder procedureBuilder = new ProcedureBuilder(null, targetProcedure.getAudit());
            procedureBuilder.setId(uniqueId);

            //we always have a performed date, so no error handling required
            DateTimeType procedureDateTime = new DateTimeType(targetProcedure.getDtPerformed());
            procedureBuilder.setPerformed(procedureDateTime);

            if (targetProcedure.getDtEnded() != null) {
                DateTimeType dt = new DateTimeType(targetProcedure.getDtEnded());
                procedureBuilder.setEnded(dt);
            }

            // set the patient reference
            Integer personId = targetProcedure.getPersonId();
            if (personId == null) {
                TransformWarnings.log(LOG, csvHelper, "Missing person ID in procedure_target for Procedure Id: {}", uniqueId);
                continue;
            }
            Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
            procedureBuilder.setPatient(patientReference);

            // status is always completed
            procedureBuilder.setStatus(Procedure.ProcedureStatus.COMPLETED);

            // set the encounter reference
            Integer encounterId = targetProcedure.getEncounterId();
            if (encounterId != null) {
                Reference encounterReference
                        = ReferenceHelper.createReference(ResourceType.Encounter, String.valueOf(encounterId));
                procedureBuilder.setEncounter(encounterReference);
            }

            // performer and recorder
            Integer performerPersonnelId = targetProcedure.getPerformerPersonnelId();
            if (performerPersonnelId != null) {
                Reference practitionerPerformerReference
                        = ReferenceHelper.createReference(ResourceType.Practitioner, String.valueOf(performerPersonnelId));
                procedureBuilder.addPerformer(practitionerPerformerReference);
            }

            Integer recordedByPersonneId = targetProcedure.getRecordedByPersonnelId();
            if (recordedByPersonneId != null) {
                Reference practitionerRecorderReference
                        = ReferenceHelper.createReference(ResourceType.Practitioner, String.valueOf(recordedByPersonneId));
                procedureBuilder.setRecordedBy(practitionerRecorderReference);
            }

            // coded concept
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);

            // can be either of these three coded types
            String procedureCodeType = targetProcedure.getProcedureType().trim();
            if (procedureCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED)
                    || procedureCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED_CT)) {

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT);

            } else if (procedureCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4);

            } else if (procedureCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_CERNER)) {
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_BARTS_CERNER_CODE_ID);
            } else {
                throw new TransformException("Unknown Procedure Target code type [" + procedureCodeType + "]");
            }
            String procedureCode = targetProcedure.getProcedureCode();
            if (!Strings.isNullOrEmpty(procedureCode)) {

                codeableConceptBuilder.setCodingCode(procedureCode);
            } else {
                TransformWarnings.log(LOG, csvHelper, "Procedure Code is empty in Procedure Target for Procedure Id: {}", uniqueId);
                continue;
            }
            String procedureTerm = targetProcedure.getProcedureTerm();
            codeableConceptBuilder.setCodingDisplay(procedureTerm);
            codeableConceptBuilder.setText(procedureTerm);

            // notes / free text
            String freeText = targetProcedure.getFreeText();
            if (!Strings.isNullOrEmpty(freeText)) {
                procedureBuilder.addNotes(freeText);
            }
            // qualifier text as more notes
            String qualifierText = targetProcedure.getQualifier();
            if (!Strings.isNullOrEmpty(qualifierText)) {
                procedureBuilder.addNotes("Qualifier: " + qualifierText);
            }

            //We receive Cerner coded Service Resources (221) as Procedure locations, so lookup and assign as a
            //Location reference if a valid integer code.  If simple free text, append to Procedure notes instead.
            //The Location resources are created in the CVREFTransform
            String locationText = targetProcedure.getLocation();
            if (!Strings.isNullOrEmpty(locationText)) {

                try {
                        // is the location an Integer Id?
                        Integer.parseInt(locationText);

                        //create a reference using the code (locationText) which has been validated as an Integer
                        Reference procedureLocation
                            = ReferenceHelper.createReference(ResourceType.Location, locationText);
                        procedureBuilder.setLocation(procedureLocation);

                     } catch (NumberFormatException ex) {
                        //the location is text only, so set as notes
                        procedureBuilder.addNotes("Location(s): " + locationText);
                    }
            }

            // this is the speciality group code of the surgeon
            String specialtyCode = targetProcedure.getSpecialty();
            if (!Strings.isNullOrEmpty(specialtyCode)) {

                CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(SURGEON_SPECIALITY_GROUP, specialtyCode);
                if (codeRef != null) {
                    String specialtyGroupTerm = codeRef.getCodeDispTxt();

                    procedureBuilder.setSpecialtyGroup(specialtyGroupTerm);
                }
            }

            // sequence number, primary and parent procedure
            Integer sequenceNumber = targetProcedure.getProcedureSeqNbr();
            if (sequenceNumber != null) {
                procedureBuilder.setSequenceNumber(sequenceNumber);
                if (sequenceNumber == 1) {
                    procedureBuilder.setIsPrimary(true);

                } else {
                    // parent resource
                    String parentProcedureId = targetProcedure.getParentProcedureUniqueId();
                    if (!Strings.isNullOrEmpty(parentProcedureId)) {

                        Reference parentProcedureReference = ReferenceHelper.createReference(ResourceType.Procedure, parentProcedureId);
                        procedureBuilder.setParentResource(parentProcedureReference);
                    }
                }
            }

            if (targetProcedure.isConfidential() != null
                    && targetProcedure.isConfidential().booleanValue()) {
                procedureBuilder.setIsConfidential(true);
            }

            fhirResourceFiler.savePatientResource(null, procedureBuilder);

            //LOG.debug("Transforming procedureId: "+uniqueId+"  Filed");
        }
    }

    public static Date setTime235959(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }
//
//
//    public static void createProcedure(PROCE parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {
//
//        // this Procedure resource id
//        CsvCell procedureIdCell = parser.getProcedureID();
//
//        //if the record is non-active (i.e. deleted) we ONLY get the ID, date and active indicator, NOT the person ID
//        //so we need to re-retrieve the previous instance of the resource to find the patient Reference which we need to delete
//        CsvCell activeCell = parser.getActiveIndicator();
//        if (!activeCell.getIntAsBoolean()) {
//
//            Procedure existingProcedure = (Procedure) csvHelper.retrieveResourceForLocalId(ResourceType.Procedure, procedureIdCell);
//            if (existingProcedure != null) {
//                ProcedureBuilder procedureBuilder = new ProcedureBuilder(existingProcedure);
//                //remember to pass in false since this procedure is already ID mapped
//                procedureBuilder.setDeletedAudit(activeCell);
//                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, procedureBuilder);
//            }
//
//            return;
//        }
//
//        // get encounter details (should already have been created previously)
//        CsvCell encounterIdCell = parser.getEncounterId();
//        String personId = csvHelper.findPersonIdFromEncounterId(encounterIdCell);
//
//        if (personId == null) {
//            //TransformWarnings.log(LOG, parser, "Skipping Procedure {} due to missing encounter", procedureIdCell.getString());
//            return;
//        }
//        CsvCell procedureDateTimeCell = parser.getProcedureDateTime();
//
//        // create the FHIR Procedure
//        ProcedureBuilder procedureBuilder = new ProcedureBuilder();
//        procedureBuilder.setId(procedureIdCell.getString(), procedureIdCell);
//        String conceptCode;
//        Date d = BartsCsvHelper.parseDate(procedureDateTimeCell);
//        DateTimeType dateTimeType = new DateTimeType(d);
//        procedureBuilder.setPerformed(dateTimeType, procedureDateTimeCell);
//
//
//        // set the patient reference
//        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId);
//        procedureBuilder.setPatient(patientReference); //we don't have a source cell to audit with, since this came from the Encounter
//
//        procedureBuilder.setStatus(Procedure.ProcedureStatus.COMPLETED);
//        CsvCell personIdCell = CsvCell.factoryDummyWrapper(personId);
////        EncounterBuilder encounterBuilder = csvHelper.getEncounterCache().borrowEncounterBuilder(encounterIdCell, personIdCell, activeCell);
//
//
//        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterIdCell.getString());
//        procedureBuilder.setEncounter(encounterReference, encounterIdCell);
//
//        CsvCell encounterSliceIdCell = parser.getEncounterSliceID();
//        if (!BartsCsvHelper.isEmptyOrIsZero(encounterSliceIdCell)) {
//            IdentifierBuilder identifierBuilder = new IdentifierBuilder(procedureBuilder);
//            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
//            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_ENCOUNTER_SLICE_ID);
//            identifierBuilder.setValue(encounterSliceIdCell.getString(), encounterSliceIdCell);
//        }
//
//        CsvCell personnelIdCell = parser.getPersonnelId();
//        //this cell is always empty at Barts, but adding validation to ensure that stays the case. If this changes,
//        //we'll need to start processing this field
//        if (!BartsCsvHelper.isEmptyOrIsZero(personnelIdCell)) {
//            throw new TransformException("PROCEDURE_HCP_PRSNL_ID column is not empty in PROCE file");
//        }
//
//        // Procedure is coded either Snomed or OPCS4
//        CsvCell conceptIdentifierCell = parser.getConceptCodeIdentifier();
//        if (conceptIdentifierCell.isEmpty()) {
//            //got some bad data from the original bulk, which contain no concept code for a handful of 2012 records - so ignore these records
//            TransformWarnings.log(LOG, csvHelper, "CONCEPT_CKI_IDENT is empty in PROCE file for Procedure ID {}", procedureIdCell);
//            return;
//            //throw new TransformException("CONCEPT_CKI_IDENT is empty in PROCE file for Procedure ID " + procedureIdCell);
//        }
//
//        conceptCode = csvHelper.getProcedureOrDiagnosisConceptCode(conceptIdentifierCell);
//        String conceptCodeType = csvHelper.getProcedureOrDiagnosisConceptCodeType(conceptIdentifierCell);
//
//        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
//
//        if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED)) {
//            //NOTE: this code IS a SNOMED concept ID, unlike the Problem file which has a description ID
//            if (BartsCsvHelper.isEmptyOrIsEndOfTime(procedureDateTimeCell)) {
//                //if there's no datetime, we've been told to ignore these records
//                TransformWarnings.log(LOG, parser, "No Procedure_dt_time in PROCE file for SNOMED Procedure ID {}", procedureIdCell);
//                return;
//            }
//
//            String term = TerminologyService.lookupSnomedTerm(conceptCode);
//            if (Strings.isNullOrEmpty(term)) {
//                TransformWarnings.log(LOG, csvHelper, "Failed to find Snomed term for {}", conceptIdentifierCell);
//            }
//
//            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, conceptIdentifierCell);
//            codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
//            codeableConceptBuilder.setCodingDisplay(term); //don't pass in a cell as this was derived
//            codeableConceptBuilder.setText(term); //don't pass in a cell as this was derived
//
//        } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_ICD_10)) {
//            String term = TerminologyService.lookupIcd10CodeDescription(conceptCode);
//            if (Strings.isNullOrEmpty(term)) {
//                TransformWarnings.log(LOG, csvHelper, "Failed to find ICD-10 term for {}", conceptIdentifierCell);
//            }
//
//            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10, conceptIdentifierCell);
//            codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
//            codeableConceptBuilder.setCodingDisplay(term); //don't pass in a cell as this was derived
//            codeableConceptBuilder.setText(term); //don't pass in a cell as this was derived
//
//        } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
//            String term = TerminologyService.lookupOpcs4ProcedureName(conceptCode);
//            if (Strings.isNullOrEmpty(term)) {
//                TransformWarnings.log(LOG, csvHelper, "Failed to find OPCS-4 term for {}", conceptIdentifierCell);
//            }
//            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4, conceptIdentifierCell);
//            codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
//            codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
//            codeableConceptBuilder.setText(term); //don't pass in the cell as this is derived
//        } else {
//            throw new TransformException("Unknown PROCE code type [" + conceptCodeType + "]");
//        }
//
//        //use the sequence number to work out which is the primary procedure and link secondary procedures to their primary
//        //note, this is only applicable to OPCS-4 procedures - for Snomed procedures the sequence number is always zero
//        processSequenceNumber(parser, procedureBuilder, csvHelper);
//
//        //populate comments, performed date, consultant etc. from that file if possible
//        if (parser.getEncounterId().getString() != null) {
//            String compatibleEncId = parser.getEncounterId().getString() + TWO_DECIMAL_PLACES; //Procedure has encounter ids suffixed with .00.
//            String procCode = conceptIdentifierCell.getString().substring(conceptIdentifierCell.getString().indexOf("!") + 1); // before the ! is the code scheme.
//            if (parser.getProcedureTypeCode().getString().equals(BartsCsvHelper.CODE_TYPE_SNOMED)) {
//                //SNOMED entries use the SNOMED description id rather than concept code
//                codeableConceptBuilder.setText(procCode);
//                procCode = csvHelper.lookupSnomedConceptIdFromDescId(procCode);
//            }
//
//            CsvCell sequenceNumberCell = parser.getCDSSequence();
//
//            List<String> procCodes = new ArrayList<>();
//            // Get data from SUS file caches for OPCS4
//            if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
//                // Link to records cached from SUSInpatientTail
//                List<SusTailCacheEntry> tailCacheList = new ArrayList<>();
//                tailCacheList = csvHelper.getSusPatientTailCache().getPatientByEncId(parser.getEncounterId().getString());
//                if (tailCacheList != null && tailCacheList.size() > 0) {  // we need the patient tail records to link
//                    List<String> csdIds = new ArrayList<>();
//                    for (SusTailCacheEntry e : tailCacheList) {
//                        if (!BartsCsvHelper.isEmptyOrIsZero(sequenceNumberCell)
//                                && !e.getCDSUniqueIdentifier().isEmpty()) {
//                            csdIds.add(e.getCDSUniqueIdentifier().getString());
//                        }
//                    }
//                    // Use tail records from above to link to SUSInpatient records
//                    List<SusPatientCacheEntry> patientCacheList = new ArrayList<>();
//                    SusPatientCache patientCache = csvHelper.getSusPatientCache();
//                    for (String id : csdIds) {
//                        if (patientCache.csdUIdInCache(id)) {
//                            SusPatientCacheEntry susPatientCacheEntry = patientCache.getPatientByCdsUniqueId(id);
//                            int seqNo = sequenceNumberCell.getInt();
//                            switch (seqNo) {
//                                case 1:
//                                    if (conceptCode.equals(susPatientCacheEntry.getPrimaryProcedureOPCS().getString()))
//                                        patientCacheList.add(susPatientCacheEntry);
//                                    procCodes.add(conceptCode);
//                                    if (BartsCsvHelper.isEmptyOrIsEndOfTime(procedureDateTimeCell)
//                                            && susPatientCacheEntry.getPrimaryProcedureDate() != null
//                                            && !susPatientCacheEntry.getPrimaryProcedureDate().isEmpty()
//                                            && susPatientCacheEntry.getPrimaryProcedureDate().getDate() != null) {
//                                        DateTimeType dt = new DateTimeType(susPatientCacheEntry.getPrimaryProcedureDate().getDate());
//                                        procedureBuilder.setPerformed(dt, susPatientCacheEntry.getPrimaryProcedureDate());
//                                    }
//                                    break;
//                                case 2:
//                                    if (conceptCode.equals(susPatientCacheEntry.getSecondaryProcedureOPCS().getString()))
//                                        patientCacheList.add(susPatientCacheEntry);
//                                    procCodes.add(conceptCode);
//                                    if (BartsCsvHelper.isEmptyOrIsEndOfTime(procedureDateTimeCell)
//                                            && susPatientCacheEntry.getSecondaryProcedureDate() != null
//                                            && !susPatientCacheEntry.getSecondaryProcedureDate().isEmpty()
//                                            && susPatientCacheEntry.getSecondaryProcedureDate().getDate() != null) {
//                                        DateTimeType dt = new DateTimeType(susPatientCacheEntry.getSecondaryProcedureDate().getDate());
//                                        procedureBuilder.setPerformed(dt, susPatientCacheEntry.getSecondaryProcedureDate());
//                                    }
//                                    break;
//                                default:
//                                    if (!susPatientCacheEntry.getOtherCodes().isEmpty()
//                                            && susPatientCacheEntry.getOtherCodes().get(seqNo).equals(conceptCode)) {
//                                        procCodes.add(conceptCode);
//                                        patientCacheList.add(susPatientCacheEntry);
//                                        if (BartsCsvHelper.isEmptyOrIsEndOfTime(procedureDateTimeCell)
//                                                && susPatientCacheEntry.getOtherDates() != null
//                                                && !susPatientCacheEntry.getOtherDates().isEmpty()) {
//                                            DateTimeType dt = new DateTimeType(susPatientCacheEntry.getOtherDates().get(seqNo));
//                                            procedureBuilder.setPerformed(dt, susPatientCacheEntry.getOtherSecondaryProceduresOPCS());
//                                        }
//                                        break;
//                                    }
//                            }
//                            patientCacheList.add(susPatientCacheEntry);
//                        }
//                    } // Now we have lists of candidate SUS Patient and patient tail records. Now parse them.
//                    //LOG.info("Procedure " + procedureIdCell.getString() + " SUS patient record count:" + patientCacheList.size());
//
//                    List<String> knownPerformers = new ArrayList<>(); // Track known performers to avoid duplicate performers per procedure.
//                    int performerCount = 0;
//                    for (SusTailCacheEntry tail : tailCacheList) {
//                        if (!procedureBuilder.hasPerformer()
//                                && !tail.getResponsibleHcpPersonnelId().getString().isEmpty()
//                                && !tail.getResponsibleHcpPersonnelId().isEmpty()
//                                && !knownPerformers.contains(tail.getResponsibleHcpPersonnelId())) {
//                            Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, tail.getResponsibleHcpPersonnelId().getString());
//                            // LOG.info("Adding performer " + tail.getResponsibleHcpPersonnelId() + " to procedure " + procedureIdCell.getString());
//                            procedureBuilder.addPerformer(practitionerReference, personnelIdCell);
//                            knownPerformers.add(tail.getResponsibleHcpPersonnelId().getString());
//                            performerCount++;
//
//                        }
//                    }
//                    //LOG.info("Procedure " + procedureIdCell.getString() + ". Performers added:" + performerCount);
//                } else {
//                    TransformWarnings.log(LOG, csvHelper, "No tail records found for person {}, procedure {} ", personId, procedureIdCell.getString());
//                }
//            }
////            ProcedurePojo pojo = csvHelper.getProcedureCache().getProcedurePojoByMultipleFields(compatibleEncId, procCode,
////                    BartsCsvHelper.parseDate(procedureDateTimeCell));
//            List<ProcedurePojo> pojoList = csvHelper.getProcedureCache().getProcedurePojoByEncId(compatibleEncId);
//            ProcedurePojo pojo = null;
//            if (pojoList != null) {
//                for (ProcedurePojo p : pojoList) {
//                    if (procCodes.contains(p.getProcedureCode().getString())
//                            && isSameDay(p.getProc_dt_tm().getDate(),procedureDateTimeCell.getDate())) {
//                        //&& p.getProc_dt_tm().getDate().equals(procedureDateTimeCell.getDate())) {
//                        pojo = p;
//                        break;
//                    }
//                }
//            } else {
//                TransformWarnings.log(LOG, csvHelper, "Failed to find matching Enctr Id {} for {} procedure", compatibleEncId, procedureIdCell.getString());
//            }
//            if (pojo != null) {
//                if (!procedureBuilder.hasPerformer() && pojo.getConsultant() != null) {
//                    CsvCell consultantCell = pojo.getConsultant();
//                    if (!consultantCell.isEmpty()) {
//                        String consultantStr = consultantCell.getString();
//                        String personnelIdStr = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, consultantStr);
//                        if (Strings.isNullOrEmpty(personnelIdStr)) {
//                            TransformWarnings.log(LOG, csvHelper, "Failed to find PRSNL ID for {}", consultantStr);
//                        } else {
//                            Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, personnelIdStr);
//                            procedureBuilder.addPerformer(practitionerReference, personnelIdCell);
//                        }
//                    }
//                }
//                if (pojo.getNotes() != null && !pojo.getNotes().isEmpty()) {
//                    procedureBuilder.addNotes(pojo.getNotes().getString());
//                }
//                if (pojo.getProc_dt_tm() != null && pojo.getProc_dt_tm().getDate() != null) {
//                    procedureBuilder.setRecordedDate(BartsCsvHelper.parseDate(pojo.getProc_dt_tm()));
//                } else {
//                    if (pojo.getCreate_dt_tm() != null && pojo.getCreate_dt_tm().getDate() != null) {
//                        procedureBuilder.setRecordedDate(BartsCsvHelper.parseDate(pojo.getCreate_dt_tm()));
//                    }
//                }
//                if (!procedureBuilder.hasPerformer() && pojo.getUpdatedBy() != null) {
//                    CsvCell updateByCell = pojo.getUpdatedBy();
//                    if (!updateByCell.isEmpty()) {
//                        String updatedByStr = updateByCell.getString();
//                        String personnelIdStr = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, updatedByStr);
//                        if (Strings.isNullOrEmpty(personnelIdStr)) {
//                            TransformWarnings.log(LOG, csvHelper, "Failed to find PRSNL ID for {}", updatedByStr);
//                        } else {
//                            Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, personnelIdStr);
//                            procedureBuilder.setRecordedBy(practitionerReference, personnelIdCell);
//                        }
//                    }
//                }
//
//            } else {
//                LOG.info("No fixed width procedure cached for encId:" + compatibleEncId + ". ProcCode:" + procCode + " at:" + BartsCsvHelper.parseDate(procedureDateTimeCell));
//            }
//        }
//
//        // save resource
//        LOG.info("Procedure pojo cache size is :" + csvHelper.getProcedureCache().size());
//        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
//    }
//
//    private static void processSequenceNumber(PROCE parser, ProcedureBuilder procedureBuilder, BartsCsvHelper csvHelper) throws Exception {
//
//        CsvCell sequenceNumberCell = parser.getCDSSequence();
//        if (BartsCsvHelper.isEmptyOrIsZero(sequenceNumberCell)) {
//            return;
//        }
//
//        procedureBuilder.setSequenceNumber(sequenceNumberCell.getInt(), sequenceNumberCell);
//
//        //sequence number ONE is primary
//        if (sequenceNumberCell.getInt() == 1) {
//            //procedureBuilder.setIsPrimary(true, sequenceNumberCell);
//            procedureBuilder.createOrUpdateIsPrimaryExtension(true, sequenceNumberCell);
//
//        } else {
//            //if secondary, use the pre-cached PROCEDURE_ID to link this procedure to its primary
//            CsvCell encounterIdCell = parser.getEncounterId();
//            String procedureIdStr = csvHelper.getInternalId(ProcedureTargetTransformer.INTERNAL_ID_MAP_PRIMARY_PROCEDURE, encounterIdCell.getString());
//
//            Reference procedureReference = ReferenceHelper.createReference(ResourceType.Procedure, procedureIdStr);
//            procedureBuilder.setParentResource(procedureReference, encounterIdCell, sequenceNumberCell);
//        }
//    }

}
