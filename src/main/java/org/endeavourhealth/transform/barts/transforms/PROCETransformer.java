package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.ProcedurePojo;
import org.endeavourhealth.transform.barts.cache.SusPatientCache;
import org.endeavourhealth.transform.barts.cache.SusPatientCacheEntry;
import org.endeavourhealth.transform.barts.cache.SusTailCacheEntry;
import org.endeavourhealth.transform.barts.schema.PROCE;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PROCETransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PROCETransformer.class);
    private static final String TWO_DECIMAL_PLACES = ".00";


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {
                try {
                    createProcedure((PROCE) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createProcedure(PROCE parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // this Procedure resource id
        CsvCell procedureIdCell = parser.getProcedureID();
        Long parentProcedureId = 0L;

        //if the record is non-active (i.e. deleted) we ONLY get the ID, date and active indicator, NOT the person ID
        //so we need to re-retrieve the previous instance of the resource to find the patient Reference which we need to delete
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {

            Procedure existingProcedure = (Procedure) csvHelper.retrieveResourceForLocalId(ResourceType.Procedure, procedureIdCell);
            if (existingProcedure != null) {
                ProcedureBuilder procedureBuilder = new ProcedureBuilder(existingProcedure);
                //remember to pass in false since this procedure is already ID mapped
                procedureBuilder.setDeletedAudit(activeCell);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, procedureBuilder);
            }

            return;
        }

        // get encounter details (should already have been created previously)
        CsvCell encounterIdCell = parser.getEncounterId();
        String personId = csvHelper.findPersonIdFromEncounterId(encounterIdCell);

        if (personId == null) {
            //TransformWarnings.log(LOG, parser, "Skipping Procedure {} due to missing encounter", procedureIdCell.getString());
            return;
        }
        CsvCell procedureDateTimeCell = parser.getProcedureDateTime();

        // create the FHIR Procedure
        ProcedureBuilder procedureBuilder = new ProcedureBuilder();
        procedureBuilder.setId(procedureIdCell.getString(), procedureIdCell);
        String conceptCode;
        Date d = BartsCsvHelper.parseDate(procedureDateTimeCell);
        DateTimeType dateTimeType = new DateTimeType(d);
        procedureBuilder.setPerformed(dateTimeType, procedureDateTimeCell);


        // set the patient reference
        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId);
        procedureBuilder.setPatient(patientReference); //we don't have a source cell to audit with, since this came from the Encounter

        procedureBuilder.setStatus(Procedure.ProcedureStatus.COMPLETED);
        CsvCell personIdCell = CsvCell.factoryDummyWrapper(personId);
//        EncounterBuilder encounterBuilder = csvHelper.getEncounterCache().borrowEncounterBuilder(encounterIdCell, personIdCell, activeCell);


        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterIdCell.getString());
        procedureBuilder.setEncounter(encounterReference, encounterIdCell);

        CsvCell encounterSliceIdCell = parser.getEncounterSliceID();
        if (!BartsCsvHelper.isEmptyOrIsZero(encounterSliceIdCell)) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(procedureBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_ENCOUNTER_SLICE_ID);
            identifierBuilder.setValue(encounterSliceIdCell.getString(), encounterSliceIdCell);
        }

        CsvCell personnelIdCell = parser.getPersonnelId();
        //this cell is always empty at Barts, but adding validation to ensure that stays the case. If this changes,
        //we'll need to start processing this field
        if (!BartsCsvHelper.isEmptyOrIsZero(personnelIdCell)) {
            throw new TransformException("PROCEDURE_HCP_PRSNL_ID column is not empty in PROCE file");
        }

        // Procedure is coded either Snomed or OPCS4
        CsvCell conceptIdentifierCell = parser.getConceptCodeIdentifier();
        if (conceptIdentifierCell.isEmpty()) {
            //got some bad data from the original bulk, which contain no concept code for a handful of 2012 records - so ignore these records
            TransformWarnings.log(LOG, csvHelper, "CONCEPT_CKI_IDENT is empty in PROCE file for Procedure ID {}", procedureIdCell);
            return;
            //throw new TransformException("CONCEPT_CKI_IDENT is empty in PROCE file for Procedure ID " + procedureIdCell);
        }

        conceptCode = csvHelper.getProcedureOrDiagnosisConceptCode(conceptIdentifierCell);
        String conceptCodeType = csvHelper.getProcedureOrDiagnosisConceptCodeType(conceptIdentifierCell);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);

        if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED)) {
            //NOTE: this code IS a SNOMED concept ID, unlike the Problem file which has a description ID
            if (BartsCsvHelper.isEmptyOrIsEndOfTime(procedureDateTimeCell)) {
                //if there's no datetime, we've been told to ignore these records
                TransformWarnings.log(LOG, parser, "No Procedure_dt_time in PROCE file for SNOMED Procedure ID {}", procedureIdCell);
                return;
            }

            String term = TerminologyService.lookupSnomedTerm(conceptCode);
            if (Strings.isNullOrEmpty(term)) {
                TransformWarnings.log(LOG, csvHelper, "Failed to find Snomed term for {}", conceptIdentifierCell);
            }

            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, conceptIdentifierCell);
            codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
            codeableConceptBuilder.setCodingDisplay(term); //don't pass in a cell as this was derived
            codeableConceptBuilder.setText(term); //don't pass in a cell as this was derived

        } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_ICD_10)) {
            String term = TerminologyService.lookupOpcs4ProcedureName(conceptCode);
            if (Strings.isNullOrEmpty(term)) {
                TransformWarnings.log(LOG, csvHelper, "Failed to find ICD-10 term for {}", conceptIdentifierCell);
            }

            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10, conceptIdentifierCell);
            codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
            codeableConceptBuilder.setCodingDisplay(term); //don't pass in a cell as this was derived
            codeableConceptBuilder.setText(term); //don't pass in a cell as this was derived

        } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
            String term = TerminologyService.lookupOpcs4ProcedureName(conceptCode);
            if (Strings.isNullOrEmpty(term)) {
                TransformWarnings.log(LOG, csvHelper, "Failed to find OPCS-4 term for {}", conceptIdentifierCell);
            }
            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4, conceptIdentifierCell);
            codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
            codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
            codeableConceptBuilder.setText(term); //don't pass in the cell as this is derived
        } else {
            throw new TransformException("Unknown PROCE code type [" + conceptCodeType + "]");
        }

        //populate comments, performed date, consultant etc. from that file if possible
        CsvCell sequenceNumberCell = parser.getCDSSequence();
        if (!BartsCsvHelper.isEmptyOrIsZero(sequenceNumberCell)) {
            procedureBuilder.setSequenceNumber(sequenceNumberCell.getInt());
            //TODO remove isPrimary later. Just use sequence number
            if (sequenceNumberCell.getInt() == 1) { //only sequence number ONE is primary
                procedureBuilder.setIsPrimary(true, sequenceNumberCell);
            } else {
                parentProcedureId = csvHelper.getPrimaryProcedureForEncounter(encounterIdCell.getLong());
            }
        }

        if (parser.getEncounterId().getString() != null) {
            String compatibleEncId = parser.getEncounterId().getString() + TWO_DECIMAL_PLACES; //Procedure has encounter ids suffixed with .00.
            String procCode = conceptIdentifierCell.getString().substring(conceptIdentifierCell.getString().indexOf("!") + 1); // before the ! is the code scheme.
            if (parser.getProcedureTypeCode().getString().equals(BartsCsvHelper.CODE_TYPE_SNOMED)) {
                //SNOMED entries use the SNOMED description id rather than concept code
                codeableConceptBuilder.setText(procCode);
                procCode = csvHelper.lookupSnomedConceptIdFromDescId(procCode);

            }
            List<String> procCodes = new ArrayList<>();
            // Get data from SUS file caches for OPCS4
            if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
                // Link to records cached from SUSInpatientTail
                List<SusTailCacheEntry> tailCacheList = new ArrayList<>();
                tailCacheList = csvHelper.getSusPatientTailCache().getPatientByEncId(parser.getEncounterId().getString());
                if (tailCacheList != null && tailCacheList.size() > 0) {  // we need the patient tail records to link
                    List<String> csdIds = new ArrayList<>();
                    for (SusTailCacheEntry e : tailCacheList) {
                        if (!BartsCsvHelper.isEmptyOrIsZero(sequenceNumberCell)
                                && !e.getCDSUniqueIdentifier().isEmpty()) {
                            csdIds.add(e.getCDSUniqueIdentifier().getString());
                        }
                    }
                    // Use tail records from above to link to SUSInpatient records
                    List<SusPatientCacheEntry> patientCacheList = new ArrayList<>();
                    SusPatientCache patientCache = csvHelper.getSusPatientCache();
                    for (String id : csdIds) {
                        if (patientCache.csdUIdInCache(id)) {
                            SusPatientCacheEntry susPatientCacheEntry = patientCache.getPatientByCdsUniqueId(id);
                            int seqNo = sequenceNumberCell.getInt();
                            switch (seqNo) {
                                case 1:
                                    if (conceptCode.equals(susPatientCacheEntry.getPrimaryProcedureOPCS().getString()))
                                        patientCacheList.add(susPatientCacheEntry);
                                    procCodes.add(conceptCode);
                                    break;
                                case 2:
                                    if (conceptCode.equals(susPatientCacheEntry.getSecondaryProcedureOPCS().getString()))
                                        patientCacheList.add(susPatientCacheEntry);
                                    procCodes.add(conceptCode);
                                   parentProcedureId = csvHelper.getPrimaryProcedureForEncounter(encounterIdCell.getLong());
                                    break;
                                default:
                                    if (!susPatientCacheEntry.getOtherCodes().isEmpty()
                                            && susPatientCacheEntry.getOtherCodes().get(seqNo).equals(conceptCode)) {
                                        procCodes.add(conceptCode);
                                        patientCacheList.add(susPatientCacheEntry);
                                        parentProcedureId = csvHelper.getPrimaryProcedureForEncounter(encounterIdCell.getLong());
                                        break;
                                    }
                            }
                            patientCacheList.add(susPatientCacheEntry);
                        }
                    } // Now we have lists of candidate SUS Patient and patient tail records. Now parse them.
                    LOG.info("Procedure " + procedureIdCell.getString() + " SUS patient record count:" + patientCacheList.size());

                    List<String> knownPerformers = new ArrayList<>(); // Track known performers to avoid duplicate performers per procedure.
                    int performerCount = 0;
                    for (SusTailCacheEntry tail : tailCacheList) {
                        if (!procedureBuilder.hasPerformer()
                                && !tail.getResponsibleHcpPersonnelId().getString().isEmpty()
                                && !tail.getResponsibleHcpPersonnelId().isEmpty()
                                && !knownPerformers.contains(tail.getResponsibleHcpPersonnelId())) {
                            Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, tail.getResponsibleHcpPersonnelId().getString());
                            LOG.info("Adding performer " + tail.getResponsibleHcpPersonnelId() + " to procedure " + procedureIdCell.getString());
                            procedureBuilder.addPerformer(practitionerReference, personnelIdCell);
                            knownPerformers.add(tail.getResponsibleHcpPersonnelId().getString());
                            performerCount++;
                            if (BartsCsvHelper.isEmptyOrIsEndOfTime(procedureDateTimeCell)
                                    && tail.getCdsActivityDate() != null && !tail.getCdsActivityDate().isEmpty()
                                    && tail.getCdsActivityDate().getDate() != null) {
                                DateTimeType dt = new DateTimeType(tail.getCdsActivityDate().getDate());
                                procedureBuilder.setPerformed(dt, tail.getCdsActivityDate());
                            }
                        }
                    }
                    LOG.info("Procedure " + procedureIdCell.getString() + ". Performers added:" + performerCount);
                } else {
                    TransformWarnings.log(LOG, csvHelper, "No tail records found for person {}, procedure {} ", personId, procedureIdCell.getString());
                }
            }
            if (!parentProcedureId.equals(null) && !parentProcedureId.equals(0L)) {
                Reference procedureReference =  ReferenceHelper.createReference(ResourceType.Procedure, parentProcedureId.toString());
                procedureBuilder.setParentResource(procedureReference);
            }
//            ProcedurePojo pojo = csvHelper.getProcedureCache().getProcedurePojoByMultipleFields(compatibleEncId, procCode,
//                    BartsCsvHelper.parseDate(procedureDateTimeCell));
            List<ProcedurePojo> pojoList = csvHelper.getProcedureCache().getProcedurePojoByEncId(compatibleEncId);
            ProcedurePojo pojo = null;
            if (pojoList != null) {
                for (ProcedurePojo p : pojoList) {
                    if (procCodes.contains(p.getProcedureCode().getString())
                            && p.getProc_dt_tm().getDate().equals(procedureDateTimeCell.getDate())) {
                        pojo = p;
                        break;
                    }
                }
            } else {
                TransformWarnings.log(LOG, csvHelper, "Failed to find matching Enctr Id {} for {} procedure", compatibleEncId, procedureIdCell.getString());
            }
            if (pojo != null) {
                if (!procedureBuilder.hasPerformer() && pojo.getConsultant() != null) {
                    CsvCell consultantCell = pojo.getConsultant();
                    if (!consultantCell.isEmpty()) {
                        String consultantStr = consultantCell.getString();
                        String personnelIdStr = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, consultantStr);
                        if (Strings.isNullOrEmpty(personnelIdStr)) {
                            TransformWarnings.log(LOG, csvHelper, "Failed to find PRSNL ID for {}", consultantStr);
                        } else {
                            Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, personnelIdStr);
                            procedureBuilder.addPerformer(practitionerReference, personnelIdCell);
                        }
                    }
                }
                if (pojo.getNotes() != null && !pojo.getNotes().isEmpty()) {
                    procedureBuilder.addNotes(pojo.getNotes().getString());
                }
                if (pojo.getProc_dt_tm() != null && pojo.getProc_dt_tm().getDate() != null) {
                    procedureBuilder.setRecordedDate(BartsCsvHelper.parseDate(pojo.getProc_dt_tm()));
                } else {
                    if (pojo.getCreate_dt_tm() != null && pojo.getCreate_dt_tm().getDate() != null) {
                        procedureBuilder.setRecordedDate(BartsCsvHelper.parseDate(pojo.getCreate_dt_tm()));
                    }
                }
                if (!procedureBuilder.hasPerformer() && pojo.getUpdatedBy() != null && pojo.getProc_dt_tm().getDate() != null) {
                    CsvCell updateByCell = pojo.getUpdatedBy();
                    if (!updateByCell.isEmpty()) {
                        String updatedByStr = updateByCell.getString();
                        String personnelIdStr = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, updatedByStr);
                        if (Strings.isNullOrEmpty(personnelIdStr)) {
                            TransformWarnings.log(LOG, csvHelper, "Failed to find PRSNL ID for {}", updatedByStr);
                        } else {
                            Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, personnelIdStr);
                            procedureBuilder.setRecordedBy(practitionerReference, personnelIdCell);
                        }
                    }
                }

            } else {
                LOG.info("No fixed width procedure cached for encId:" + compatibleEncId + ". ProcCode:" + procCode + " at:" + BartsCsvHelper.parseDate(procedureDateTimeCell));
            }
        }

        // save resource
        LOG.info("Procedure pojo cache size is :" + csvHelper.getProcedureCache().size());
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
    }

}
