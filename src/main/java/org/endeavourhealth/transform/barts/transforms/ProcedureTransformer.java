package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProcedureTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedureTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((org.endeavourhealth.transform.barts.schema.Procedure)parser, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(org.endeavourhealth.transform.barts.schema.Procedure parser, BartsCsvHelper csvHelper) throws Exception {

        //TODO - work out unique ID
        /*ProcedureBuilder procedureBuilder = csvHelper.getProcedureCache().borrowProcedureBuilder(csvHelper, uniqueId, );

        CsvCell mrnCell = parser.getMrn();
        String personId = csvHelper.getInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, mrnCell.getString());
        if (Strings.isNullOrEmpty(personId)) {
            throw new TransformException("Failed to find person ID for MRN " + mrnCell);
        }

        if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
            return;
        }

        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId);
        if (procedureBuilder.isIdMapped()) {
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, csvHelper);
        }
        procedureBuilder.setPatient(patientReference, mrnCell);

        CsvCell encounterIdCell = parser.getEncounterId();
        String encounterId = encounterIdCell.getString();

        //the encounter ID has a trailing .00 in the source file, so strip that off here
        int index = encounterId.indexOf(".");
        encounterId = encounterId.substring(0, index);

        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterId);
        if (procedureBuilder.isIdMapped()) {
            encounterReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(encounterReference, csvHelper);
        }
        procedureBuilder.setEncounter(encounterReference, encounterIdCell);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);

        //set main codeable concept text to the raw text from the record
        CsvCell codeDescCell = parser.getProcedureText();
        String codeDesc = codeDescCell.getString();
        codeableConceptBuilder.setText(codeDesc, codeDescCell);

        CsvCell procedureCodeCell = parser.getProcedureCode();
        String procedureCode = procedureCodeCell.getString();

        CsvCell procedureCodeTypeCell = parser.getProcedureCodeType();
        String codeType = procedureCodeTypeCell.getString();
        if (codeType.equals("SNOMED CT")) {

            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, procedureCodeTypeCell);
            codeableConceptBuilder.setCodingCode(procedureCode, procedureCodeCell);

            //look up the preferred term for the code, and store that in the coding if found
            SnomedCode snomedCode = TerminologyService.lookupSnomedFromConceptId(procedureCode);
            if (snomedCode != null) {
                codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm(), procedureCodeCell);
            } else {
                codeableConceptBuilder.setCodingDisplay(codeDesc, codeDescCell);
            }

        } else if (codeType.equals("OPCS4")) {

            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4, procedureCodeTypeCell);
            codeableConceptBuilder.setCodingCode(procedureCode, procedureCodeCell);

            //look up the official term for this OPCS-4 code
            String officialTerm = TerminologyService.lookupOpcs4ProcedureName(procedureCode);
            if (officialTerm != null) {
                codeableConceptBuilder.setCodingDisplay(officialTerm, procedureCodeCell);
            } else {
                codeableConceptBuilder.setCodingDisplay(codeDesc, codeDescCell);
            }

        } else {
            throw new TransformException("Unexpected procedure code type " + procedureCodeTypeCell);
        }

        CsvCell commentsCell = parser.getComment();
        if (!commentsCell.isEmpty()) {
            String comment = commentsCell.getString();
            procedureBuilder.addNotes(comment, commentsCell);
        }

        CsvCell performedDateCell = parser.getProcedureDateTime();
        Date performedDate = performedDateCell.getDateTime();
        procedureBuilder.setPerformed(new DateTimeType(performedDate), performedDateCell);

        CsvCell recordedDateCell = parser.getCreateDateTime();
        Date recordedDate = recordedDateCell.getDateTime();
        procedureBuilder.setRecordedDate(recordedDate, recordedDateCell);

        //I believe that anything codes is already completed
        procedureBuilder.setStatus(Procedure.ProcedureStatus.COMPLETED);

        CsvCell performerCell = parser.getConsultant();
        String performerStr = performerCell.getString();

        //the two practitioners are listed with their full names in the file, so we need to perform reverse
        //lookups to find the practitioner IDs for them
        String performerPersonnelId = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, performerStr);
        if (Strings.isNullOrEmpty(performerPersonnelId)) {
            throw new TransformException("Failed to find personnel ID for free text name [" + performerStr + "]");
        }
        Reference performerReference = ReferenceHelper.createReference(ResourceType.Practitioner, performerPersonnelId);
        if (procedureBuilder.isIdMapped()) {
            performerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(performerReference, csvHelper);
        }
        procedureBuilder.addPerformer(performerReference, performerCell);

        CsvCell recordedCell = parser.getUpdatedBy();
        String recordedStr = recordedCell.getString();

        String recordedPersonnelId = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, recordedStr);
        if (Strings.isNullOrEmpty(recordedPersonnelId)) {
            throw new TransformException("Failed to find personnel ID for free text name [" + recordedStr + "]");
        }
        Reference recordedReference = ReferenceHelper.createReference(ResourceType.Practitioner, recordedPersonnelId);
        if (procedureBuilder.isIdMapped()) {
            recordedReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(recordedReference, csvHelper);
        }
        procedureBuilder.setRecordedBy(recordedReference, recordedCell);




        //TODO - store/update the Encounter-FIN mappings, since this seems a good source

        csvHelper.getProcedureCache().returnProcedureBuilder(uniqueId, procedureBuilder);*/


    }


    /*public static final DateFormat resourceIdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {

            while (parser.nextRecord()) {
                try {
                    createProcedure((org.endeavourhealth.transform.barts.schema.Procedure)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createProcedure(org.endeavourhealth.transform.barts.schema.Procedure parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Organisation
        Address fhirOrgAddress = AddressHelper.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Patient
        ResourceId patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null, null, null, null, organisationResourceId, null, null, null, null, null);
        // EpisodeOfCare - Procedure record cannot be linked to an EpisodeOfCare
        // Encounter
        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE,  parser.getEncounterId().toString());
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterId().toString());
        }

        createEncounter(parser.getCurrentState(),  fhirResourceFiler, patientResourceId, null,  encounterResourceId, Encounter.EncounterState.FINISHED, parser.getAdmissionDateTime(), parser.getDischargeDateTime(), null, Encounter.EncounterClass.INPATIENT);

        // this Diagnosis resource id
        ResourceId procedureResourceId = getProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterId().toString(), parser.getProcedureDateTimeAsString(), parser.getProcedureCode(), 0);

        // Procedure Code
        //CodeableConcept procedureCode = new CodeableConcept();
        //procedureCode.addCoding().setSystem(getCodeSystemName(FhirCodeUri.CODE_SYSTEM_CERNER_SNOMED)).setDisplay(parser.getProcedureText()).setCode(parser.getProcedureCode());
        CodeableConcept procedureCode = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, parser.getProcedureText(), parser.getProcedureCode());

        // Create resource

        String code = parser.getProcedureCode();
        String term = parser.getProcedureText();
        Date date = parser.getProcedureDateTime();
        String comment = parser.getComment();
        ProcedureBuilder procedureBuilder = createProcedureResource(procedureResourceId, encounterResourceId, patientResourceId, Procedure.ProcedureStatus.COMPLETED, code, term, FhirCodeUri.CODE_SYSTEM_SNOMED_CT, date, comment, null, "clinical coding");

        // save resource
        LOG.debug("Save Procedure(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(procedureBuilder.getResource()));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), procedureBuilder);

    }*/


}
