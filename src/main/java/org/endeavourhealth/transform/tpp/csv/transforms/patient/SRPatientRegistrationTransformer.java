package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.schema.RegistrationStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.cache.MedicalRecordStatusCacheObject;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.helpers.cache.TppRecordStatusCache;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientRegistration;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SRPatientRegistrationTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRPatientRegistrationTransformer.class);

    public static final String PATIENT_ID_TO_ACTIVE_EPISODE_ID = "PatientIdToActiveEpisodeId";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRPatientRegistration.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRPatientRegistration) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(SRPatientRegistration parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();

        //because we might receive an update to this record without a change in SRRecordStatus
        //we need to retrieve the latest instance and update, so we don't lose that status
        EpisodeOfCareBuilder episodeBuilder = null;
        EpisodeOfCare episodeOfCare = (EpisodeOfCare) csvHelper.retrieveResource(rowIdCell.getString(), ResourceType.EpisodeOfCare);
        if (episodeOfCare == null) {
            episodeBuilder = new EpisodeOfCareBuilder();
            episodeBuilder.setId(rowIdCell.getString(), rowIdCell);

        } else {
            episodeBuilder = new EpisodeOfCareBuilder(episodeOfCare);
        }

        //if episode is deleted
        CsvCell removeDataCell = parser.getRemovedData();
        if (removeDataCell != null && removeDataCell.getIntAsBoolean()) {
            //only bother trying to delete if it's already been ID mapped (i.e. already saved), otherwise just discard it
            if (episodeBuilder.isIdMapped()) {
                episodeBuilder.setDeletedAudit(removeDataCell);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, episodeBuilder);
            }
            return;
        }

        //the TPP feed supplies details of registrations elsewhere, which should not be saved within the scope
        //of the publisher, as it ends up creating a lot of confusion as episodes of care are expected to be ABOUT
        //the publisher service and not somewhere else
        CsvCell orgIdCell = parser.getIDOrganisationVisibleTo();
        if (!shouldSaveEpisode(parser.getIDOrganisation(), orgIdCell)) {
            //only bother trying to delete if it's already been ID mapped (i.e. already saved), otherwise just discard it
            //delete rather than always just skip, because we PREVIOUSLY let these through, so make sure to delete to tidy up
            if (episodeBuilder.isIdMapped()) {
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, episodeBuilder);
            }
            return;
        }

        CsvCell patientIdCell = parser.getIDPatient();
        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        if (episodeBuilder.isIdMapped()) {
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, csvHelper);
        }
        episodeBuilder.setPatient(patientReference, patientIdCell);

        CsvCell regStartDateCell = parser.getDateRegistration();
        if (!regStartDateCell.isEmpty()) {
            episodeBuilder.setRegistrationStartDate(regStartDateCell.getDate(), regStartDateCell);
        }

        CsvCell regEndDateCell = parser.getDateDeRegistration();
        if (!regEndDateCell.isEmpty()) {
            episodeBuilder.setRegistrationEndDate(regEndDateCell.getDate(), regEndDateCell);
        }

        CsvCell regTypeCell = parser.getRegistrationStatus();
        RegistrationType regType = null;
        if (!regTypeCell.isEmpty()) {
            regType = mapToFhirRegistrationType(regTypeCell);
            episodeBuilder.setRegistrationType(regType, regEndDateCell);
        }

        //for GMS registrations we also may have
        if (regType != null
                && regType == RegistrationType.REGULAR_GMS) {

            List<MedicalRecordStatusCacheObject> statusesForEpisode = csvHelper.getRecordStatusHelper().getMedicalRecordStatusesForEpisode(patientIdCell, episodeBuilder);
            TppRecordStatusCache.addRecordStatuses(statusesForEpisode, episodeBuilder);
        }

        //if we have a temporary registration type, we can also work out the registration status from the reg type String too
        if (regType != null
                && regType == RegistrationType.TEMPORARY) {

            RegistrationStatus regStatus = null;

            String regTypeDesc = regTypeCell.getString();
            if (regTypeDesc.contains("Temporary Resident < 16 days")) {
                regStatus = RegistrationStatus.REGISTERED_TEMPORARY_SHORT_STAY;

            } else if (regTypeDesc.contains("Temporary Resident 16 days to 3 months")) {
                regStatus = RegistrationStatus.REGISTERED_TEMPORARY_LONG_STAY;
            }

            if (regStatus != null) {
                CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(regStatus);

                ContainedListBuilder containedListBuilder = new ContainedListBuilder(episodeBuilder);
                containedListBuilder.removeContainedList(); //remove any existing flags
                containedListBuilder.addCodeableConcept(codeableConcept, regTypeCell);
                containedListBuilder.addDateToLastItem(regStartDateCell.getDate(), regStartDateCell);
            }
        }


        Reference orgReferenceEpisode = csvHelper.createOrganisationReference(orgIdCell);
        if (episodeBuilder.isIdMapped()) {
            orgReferenceEpisode = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReferenceEpisode, csvHelper);
        }
        episodeBuilder.setManagingOrganisation(orgReferenceEpisode, orgIdCell);

        //save
        boolean mapIds = !episodeBuilder.isIdMapped();
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), mapIds, episodeBuilder);
    }

    /**
     * determines whether we should save this registration episode. If the episode is about the patient's
     * registration SOMEWHERE ELSE then we don't want to save the episode.
     */
    public static boolean shouldSaveEpisode(CsvCell orgIdCell, CsvCell idOrganisationVisibleToCell) {

        //there are a small number of records with a blank org ID, so treat them as though they were done "here"
        if (orgIdCell.isEmpty()) {
            return true;
        }

        //otherwise, if we have both IDs, make sure that the episode was done here
        String episodeAt = orgIdCell.getString();
        String publisher = idOrganisationVisibleToCell.getString();
        return episodeAt.equals(publisher);
    }


    public static RegistrationType mapToFhirRegistrationType(CsvCell regTypeCell) throws Exception {

        //there is some SystmOne legacy data where patients have multiple reg types, generally GMS + the old
        //IOS registrations (e.g. GMS,Contraception). In these cases, carry over GMS.
        List<RegistrationType> types = new ArrayList<>();

        // Main concern is whether patient is GMS, Private or temporary.
        String s = regTypeCell.getString();
        String[] toks = s.split(",");

        for (String tok : toks) {

            if (tok.equalsIgnoreCase("GMS")
                    || tok.equalsIgnoreCase("Standard")) {
                types.add(RegistrationType.REGULAR_GMS);
            } else if (tok.equalsIgnoreCase("IMMEDIATELY NECESSARY")
                    || tok.equalsIgnoreCase("Immediately Necessary Treatment")) {
                types.add(RegistrationType.IMMEDIATELY_NECESSARY);
            } else if (tok.equalsIgnoreCase("PRIVATE")) {
                types.add(RegistrationType.PRIVATE);
            } else if (tok.equalsIgnoreCase("TEMPORARY")
                    || tok.equalsIgnoreCase("Temporary Resident < 16 days")
                    || tok.equalsIgnoreCase("Temporary Resident 16 days to 3 months")
                    || tok.equalsIgnoreCase("Temporary Resident (telephone consultation)")) {
                types.add(RegistrationType.TEMPORARY);
            } else if (tok.equalsIgnoreCase("APPLIED")) {
                types.add(RegistrationType.PRE_REGISTRATION);
            } else if (tok.equalsIgnoreCase("MINOR SURGERY")) {
                types.add(RegistrationType.MINOR_SURGERY);
            } else if (tok.equalsIgnoreCase("Contraception")) {
                types.add(RegistrationType.CONTRACEPTIVE_SERVICES);
            } else if (tok.equalsIgnoreCase("Child Health Surveillance")) {
                types.add(RegistrationType.CHILD_HEALTH_SURVEILLANCE);
            } else if (tok.equalsIgnoreCase("Maternity")) {
                types.add(RegistrationType.MATERNITY_SERVICES);
            } else if (tok.equalsIgnoreCase("Walk-in Patient")) {
                types.add(RegistrationType.WALK_IN);
            } else if (tok.equalsIgnoreCase("Emergency")) {
                types.add(RegistrationType.EMERGENCY);
            } else if (tok.equalsIgnoreCase("Other")
                    || tok.equalsIgnoreCase("Patient")
                    || tok.equalsIgnoreCase("Incomplete")
                    || tok.equalsIgnoreCase("Remotely Registered")) {
                types.add(RegistrationType.OTHER);

            } else {
                throw new TransformException("Unmapped registration type " + tok);
            }
        }

        if (types.size() == 1) {
            return types.get(0);
        }

        //if we have multiple reg types, choese the most significant one
        if (types.contains(RegistrationType.REGULAR_GMS)) {
            return RegistrationType.REGULAR_GMS;

        } else if (types.contains(RegistrationType.EMERGENCY)) {
            return RegistrationType.EMERGENCY;

        } else if (types.contains(RegistrationType.PRE_REGISTRATION)) {
            return RegistrationType.PRE_REGISTRATION;

        } else if (types.contains(RegistrationType.PRIVATE)) {
            return RegistrationType.PRIVATE;

        } else if (types.contains(RegistrationType.TEMPORARY)) {
            return RegistrationType.TEMPORARY;

        } else if (types.contains(RegistrationType.IMMEDIATELY_NECESSARY)) {
            return RegistrationType.IMMEDIATELY_NECESSARY;

        } else {
            return types.get(0);
            //throw new TransformException("Don't know how to handle registration type string " + s);
        }
    }


}
