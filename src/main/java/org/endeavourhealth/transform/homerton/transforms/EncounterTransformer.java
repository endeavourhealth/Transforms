package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.endeavourhealth.transform.homerton.cache.OrganisationResourceCache;
import org.endeavourhealth.transform.homerton.schema.EncounterTable;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class EncounterTransformer extends HomertonBasisTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(EncounterTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                        createEncounter((EncounterTable) parser, fhirResourceFiler, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createEncounter(EncounterTable parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonCsvHelper csvHelper) throws Exception {

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        CsvCell activeCell = parser.getActiveInd();

        // this will allow the Procedure transform to derive the PersonId from the EncounterId in that transform
        csvHelper.cacheEncounterIdToPersonId(encounterIdCell, personIdCell);

        //EncounterBuilder encounterBuilder
        //        = csvHelper.getEncounterCache().getEncounterBuilder(encounterIdCell, csvHelper);

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(encounterIdCell.getString(), encounterIdCell);

        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personIdCell.getString());
        encounterBuilder.setPatient(patientReference, personIdCell);

        // if inactive, we want to delete it
        if (!activeCell.getIntAsBoolean()) {

            //boolean mapIds = !encounterBuilder.isIdMapped();
            //fhirResourceFiler.deletePatientResource(parser.getCurrentState(), mapIds, encounterBuilder);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), encounterBuilder);
            return;
        }

        // Identifiers
        CsvCell finIdCell = parser.getMillenniumFinancialNumberIdentifier();
        if (!finIdCell.isEmpty()) {
            List<Identifier> identifiers
                    = IdentifierBuilder.findExistingIdentifiersForSystem(encounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_HOMERTON_FIN_EPISODE_ID);
            if (identifiers.size() > 0) {
                encounterBuilder.getIdentifiers().remove(identifiers.get(0));
            }
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_HOMERTON_FIN_EPISODE_ID);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setValue(finIdCell.getString(), finIdCell);
        }

        if (!encounterIdCell.isEmpty()) {
            IdentifierBuilder.removeExistingIdentifiersForSystem(encounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_HOMERTON_ENCOUNTER_ID);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_HOMERTON_ENCOUNTER_ID);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setValue(encounterIdCell.getString(), encounterIdCell);
        }

        // Organisation, get the Homerton organisation (already created/cached in Patient)
        UUID serviceId = parser.getServiceId();
        OrganizationBuilder organizationBuilder
                = OrganisationResourceCache.getOrCreateOrganizationBuilder (serviceId, csvHelper, fhirResourceFiler, parser);
        if (organizationBuilder == null) {
            TransformWarnings.log(LOG, parser, "Error retrieving Organization resource for ServiceId: {}",
                    serviceId.toString());
            return;
        }

        // the organisation reference, i.e. Homerton
        Reference orgReference = csvHelper.createOrganisationReference(serviceId.toString());
//        if (encounterBuilder.isIdMapped()) {
//            orgReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReference,fhirResourceFiler);
//        }
        encounterBuilder.setServiceProvider(orgReference);

        // encounter start date and time
        CsvCell encounterStartDateCell = parser.getActiveStatusDateTime();
        if (!BartsCsvHelper.isEmptyOrIsStartOfTime(encounterStartDateCell)) {
            encounterBuilder.setPeriodStart(encounterStartDateCell.getDateTime(), encounterStartDateCell);
        }

        // encounter end date and time (might not be finished so check end of time)
        CsvCell encounterEndDateCell = parser.getEncounterEndDateTime();
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(encounterEndDateCell)) {
            encounterBuilder.setPeriodEnd(encounterEndDateCell.getDateTime(), encounterEndDateCell);
        }

        // recorded date
        CsvCell recordedDateCell = parser.getEncounterCreatedDateTime();
        if (!BartsCsvHelper.isEmptyOrIsStartOfTime(recordedDateCell) && !BartsCsvHelper.isEmptyOrIsEndOfTime(recordedDateCell)){

            Date dateRecorded = recordedDateCell.getDateTime();
            encounterBuilder.setRecordedDate(dateRecorded, recordedDateCell);
        }

        // participant - Needs Personnel data - not currently available (TODO)
//        CsvCell encounterDoneBy = parser.getIDDoneBy();
//        if (!encounterDoneBy.isEmpty()) {
//
//            Reference staffReference = csvHelper.createPractitionerReference(encounterDoneBy);
//            encounterBuilder.addParticipant(staffReference, EncounterParticipantType.PRIMARY_PERFORMER, encounterDoneBy);
//        }

        // class
        CsvCell encounterTypeClassCodeCell = parser.getEncounterTypeMillenniumClassCode();
        if (!encounterTypeClassCodeCell.isEmpty()) {

            Encounter.EncounterClass cls = getEncounterClass(encounterTypeClassCodeCell.getString(), parser);
            if (cls != null) {

                encounterBuilder.setClass(cls, encounterTypeClassCodeCell);
            }
        }

        // encounter type
        CsvCell encounterTypeCell = parser.getEncounterType();
        if (!encounterTypeCell.isEmpty()) {

            encounterBuilder.setType(encounterTypeCell.getString(), encounterTypeCell);
        }

        // reason
        CsvCell reasonForVisit = parser.getReasonForVisitText();
        if (!reasonForVisit.isEmpty()) {
            encounterBuilder.addReason(reasonForVisit.getString(), reasonForVisit);
        }

        // status
        CsvCell status = parser.getEncounterStatusMillenniumCode();
        if (!status.isEmpty()) {

            encounterBuilder.setStatus(getEncounterStatus(status.getString(), encounterIdCell, parser), status);
        }

        //apply any newly linked child resources (diagnosis, procedures etc.)
        ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);
        ReferenceList newLinkedResources = csvHelper.getAndRemoveNewConsultationRelationships(encounterIdCell);
        if (newLinkedResources != null) {
            //LOG.debug("Encounter " + encounterId + " has " + newLinkedResources.size() + " child resources");
            containedListBuilder.addReferences(newLinkedResources);
        }

        // Retrieve or create EpisodeOfCare?
        // TODO - requested episode info from Jonathan Black

        //and save the resource
        //boolean mapIds = !encounterBuilder.isIdMapped();
        //fhirResourceFiler.savePatientResource(parser.getCurrentState(), mapIds, encounterBuilder);
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), encounterBuilder);
    }


    /* CODE_VALUE	CODE_SET	DISPLAY
        389	        69	    	Emergency
        391	        69	        Inpatient
        392	        69	        Observation
        393	        69	        Outpatient
        395	        69	        Preadmit
        397	        69	        Recurring
        399	        69	        Wait List */
    private static Encounter.EncounterClass getEncounterClass(String millenniumCode, ParserI parser) {

        if (millenniumCode.equalsIgnoreCase("389")) { return Encounter.EncounterClass.EMERGENCY; }
        else if (millenniumCode.equalsIgnoreCase("391")) { return Encounter.EncounterClass.INPATIENT; }
        else if (millenniumCode.equalsIgnoreCase("393")) { return Encounter.EncounterClass.OUTPATIENT; }

        return Encounter.EncounterClass.OTHER;
    }

    /* CODE_VALUE	CODE_SET	DISPLAY
        854	        261	        Active
        855	        261	        Cancelled
        856	        261	        Discharged
        857	        261	        Hold
        858	        261	        Preadmit
        859	        261	        Referred
        860	        261	        Transferred
        57317	    261	        Cancelled Pending Arrival
        57318	    261	        Pending Arrival
        57319	    261	        Rejected Pending Arrival
        84675	    261		    Complete  */
    private static Encounter.EncounterState getEncounterStatus(String millenniumCode, CsvCell encounterIdCell,  ParserI parser) throws Exception {
        if (millenniumCode.equalsIgnoreCase("854")) { return Encounter.EncounterState.INPROGRESS; }
        else if (millenniumCode.equalsIgnoreCase("855")) { return Encounter.EncounterState.CANCELLED; }
        else if (millenniumCode.equalsIgnoreCase("856")) { return Encounter.EncounterState.FINISHED; }
        else if (millenniumCode.equalsIgnoreCase("857")) { return Encounter.EncounterState.INPROGRESS; }
        else if (millenniumCode.equalsIgnoreCase("858")) { return Encounter.EncounterState.PLANNED; }
        else if (millenniumCode.equalsIgnoreCase("859")) { return Encounter.EncounterState.PLANNED; }
        else if (millenniumCode.equalsIgnoreCase("860")) { return Encounter.EncounterState.INPROGRESS; }
        else if (millenniumCode.equalsIgnoreCase("57317")) { return Encounter.EncounterState.CANCELLED; }
        else if (millenniumCode.equalsIgnoreCase("57318")) { return Encounter.EncounterState.PLANNED; }
        else if (millenniumCode.equalsIgnoreCase("57319")) { return Encounter.EncounterState.CANCELLED; }
        else if (millenniumCode.equalsIgnoreCase("84675")) { return Encounter.EncounterState.FINISHED; }
        else {
            TransformWarnings.log(LOG, parser, "Millennium status-code {} not found for Encounter ID {} in file {}. Default status set to in-progress", millenniumCode, encounterIdCell.getString(), parser.getFilePath());
            return Encounter.EncounterState.INPROGRESS;
        }
    }
}
