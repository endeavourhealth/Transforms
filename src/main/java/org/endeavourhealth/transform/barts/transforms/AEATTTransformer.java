package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCacheDateRecord;
import org.endeavourhealth.transform.barts.schema.AEATT;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ObservationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class AEATTTransformer extends BartsBasisTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AEATTTransformer.class);
    private static InternalIdDalI internalIdDAL = null;

    /*
     *
     */
    public static void transform(String version,
                                 ParserI parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        if (parser == null) {
            return;
        }

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry((AEATT)parser);
                if (valStr == null) {
                    createAandEAttendance((AEATT) parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    TransformWarnings.log(LOG, parser, "Validation error: {}", valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    /*
     *
     */
    public static String validateEntry(AEATT parser) {
        return null;
    }


    /*
     *
     */
    public static void createAandEAttendance(AEATT parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {


        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        CsvCell activeCell = parser.getActiveIndicator();
        // Patient
        boolean changeOfPatient = false;
        UUID patientUuid = csvHelper.findPatientIdFromPersonId(personIdCell);
        if (patientUuid == null) {
            TransformWarnings.log(LOG, parser, "Skipping A&E attendance {} because no Person->MRN mapping {} could be found in file {}", encounterIdCell.getString(), personIdCell.getString(), parser.getFilePath());
            return;
        }
        EncounterBuilder encounterBuilder = EncounterResourceCache.getEncounterBuilder(csvHelper, encounterIdCell.getString());
        if (encounterBuilder == null
                && !activeCell.getIntAsBoolean()) {
            // skip - encounter missing but set to delete so do nothing
            return;
        }

        if (encounterBuilder == null) {
            encounterBuilder = EncounterResourceCache.createEncounterBuilder(encounterIdCell);


            EncounterResourceCacheDateRecord dateRecord = EncounterResourceCache.getEncounterDates(encounterIdCell.getString());

            if (dateRecord != null && dateRecord.getBeginDate() != null) {
                encounterBuilder.setPeriodStart(dateRecord.getBeginDate(), dateRecord.getBeginDateCell());
            }
            if (dateRecord != null && dateRecord.getEndDate() != null) {
                encounterBuilder.setPeriodStart(dateRecord.getEndDate(), dateRecord.getEndDateCell());
            }
        } else {
            // Delete existing encounter ?
            if (!activeCell.getIntAsBoolean()) {
                encounterBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
                //LOG.debug("Delete Encounter (PatId=" + personIdCell.getString() + "):" + FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
                //fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
                EncounterResourceCache.deleteEncounterBuilder(encounterBuilder);
                return;
            }


        }
        encounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

        // Has patient reference changed?
        String currentPatientUuid = ReferenceHelper.getReferenceId(encounterBuilder.getPatient());
        if (currentPatientUuid.compareToIgnoreCase(patientUuid.toString()) != 0) {
            // As of 2018-03-02 we dont appear to get any further ENCNT entries for teh minor encounter in A35 and hence the EoC reference on an encounter cannot change
            // Patient reference on Encounter resources is handled below
            // Patient reference on EpisodeOfCare resources is handled below
            changeOfPatient = true;

            List<Resource> resourceList = csvHelper.retrieveResourceByPatient(UUID.fromString(currentPatientUuid));
            for (Resource resource : resourceList) {
                if (resource instanceof Condition) {
                    ConditionBuilder builder = new ConditionBuilder((Condition) resource);
                    builder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
                    fhirResourceFiler.savePatientResource(parser.getCurrentState(), builder);
                } else if (resource instanceof Procedure) {
                    ProcedureBuilder builder = new ProcedureBuilder((Procedure) resource);
                    builder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
                    fhirResourceFiler.savePatientResource(parser.getCurrentState(), builder);
                } else if (resource instanceof Observation) {
                    ObservationBuilder builder = new ObservationBuilder((Observation) resource);
                    builder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString()), personIdCell);
                    fhirResourceFiler.savePatientResource(parser.getCurrentState(), builder);
                }
            }
        }


        //We have a number of potential events in the patient journey through A&E. We can't map all the states.
        Encounter.EncounterState encState = null;
        // Triage
        if (parser.getTriageStartDateTime().isEmpty()) {
                encState = Encounter.EncounterState.PLANNED;
            } else
        {
            if (parser.getTriageCompleteDateTime().isEmpty()) {
                encState = Encounter.EncounterState.INPROGRESS;
            } else {
                encState = Encounter.EncounterState.FINISHED;
            }
        }
        //Save triage status
        encounterBuilder.setStatus(encState, parser.getTriageStartDateTime().getDate(),
                parser.getTriageCompleteDateTime().getDate(), parser.getTriageCatNbr());
        Reference ref = ReferenceHelper.createReference(ResourceType.Practitioner, parser.getTriagePersonId().toString());
        Period triagePeriod = new Period();
        triagePeriod.setStart(parser.getTriageStartDateTime().getDate());
        triagePeriod.setEnd(parser.getTriageCompleteDateTime().getDate());
        encounterBuilder.addParticipant(ref,EncounterParticipantType.PARTICIPANT,triagePeriod, parser.getTriageCatNbr());
        encState = null; // reset
        //  First medical assessment to conclusion
        if (parser.getFirstAssessDateTime().isEmpty()) {
            encState = Encounter.EncounterState.PLANNED;
            } else
         {  // All the records I looked at had Conclusion date set. It should at least allow us to keep some kind of progress.
            if (parser.getConclusionDateTime().isEmpty()) {
                encState = Encounter.EncounterState.INPROGRESS;
            } else {
                encState = Encounter.EncounterState.FINISHED;
            }
        }
        encounterBuilder.setStatus(encState, parser.getFirstAssessDateTime().getDate(),
                parser.getConclusionDateTime().getDate(), parser.getHcpFirstAssignedPersonId());

        //Reason
        CsvCell reasonForVisit = parser.getPresentingCompTxt();
        CodeableConcept reasonForVisitText = CodeableConceptHelper.createCodeableConcept(reasonForVisit.getString());
        encounterBuilder.addReason(reasonForVisitText, reasonForVisit);

    }// end createAandEAttendance()
    }

