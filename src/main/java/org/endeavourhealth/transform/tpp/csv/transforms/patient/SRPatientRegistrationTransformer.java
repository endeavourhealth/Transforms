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
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.cache.MedicalRecordStatusCacheObject;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientRegistration;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        EpisodeOfCare episodeOfCare = (EpisodeOfCare)csvHelper.retrieveResource(rowIdCell.getString(), ResourceType.EpisodeOfCare);
        if (episodeOfCare == null) {
            episodeBuilder = new EpisodeOfCareBuilder();
            episodeBuilder.setId(rowIdCell.getString(), rowIdCell);

        } else {
            episodeBuilder = new EpisodeOfCareBuilder(episodeOfCare);
        }

        CsvCell removeDataCell = parser.getRemovedData();
        if (removeDataCell != null && removeDataCell.getIntAsBoolean()) {

            //check that this is an existing resource, i.e. retrieved with a mapped Id
            if (episodeBuilder.isIdMapped()) {

                episodeBuilder.setDeletedAudit(removeDataCell);
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

        ContainedListBuilder containedListBuilder = new ContainedListBuilder(episodeBuilder);

        //for GMS registrations we also may have
        if (regType != null
                && regType == RegistrationType.REGULAR_GMS) {
            //TODO - only want to apply these to the right episode, not all of them!
            List<MedicalRecordStatusCacheObject> statuses = csvHelper.getAndRemoveMedicalRecordStatus(patientIdCell);
            if (statuses != null) {

                for (MedicalRecordStatusCacheObject status : statuses) {
                    CsvCell statusCell = status.getStatusCell();
                    RegistrationStatus medicalRecordStatus = convertMedicalRecordStatus(statusCell);

                    CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(medicalRecordStatus);
                    containedListBuilder.addCodeableConcept(codeableConcept, statusCell);

                    CsvCell dateCell = status.getDateCell();
                    if (!dateCell.isEmpty()) {
                        containedListBuilder.addDateToLastItem(dateCell.getDateTime(), dateCell);
                    }
                }
            }


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
                containedListBuilder.addCodeableConcept(codeableConcept, regTypeCell);
            }
        }



        CsvCell orgIdCell = parser.getIDOrganisation();
        //LOG.debug("Doing episode of care " + episodeBuilder.getResourceId() + " for patient " + patientIdCell.getString() + " and org ID " + orgIdCell.getString());

        if (!orgIdCell.isEmpty()) {

            Reference orgReferenceEpisode = csvHelper.createOrganisationReference(orgIdCell);
            if (episodeBuilder.isIdMapped()) {
                orgReferenceEpisode = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReferenceEpisode, csvHelper);
            }
            episodeBuilder.setManagingOrganisation(orgReferenceEpisode, orgIdCell);

            //and we need to set a couple of fields on the patient recourd
            PatientBuilder patientBuilder = csvHelper.getPatientResourceCache().getOrCreatePatientBuilder(patientIdCell, csvHelper);
            if (patientBuilder != null) {

                Reference orgReferencePatient = csvHelper.createOrganisationReference(orgIdCell);
                if (patientBuilder.isIdMapped()) {
                    orgReferencePatient = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReferencePatient, csvHelper);
                }
                patientBuilder.setManagingOrganisation(orgReferencePatient, orgIdCell);

                //and if the patient is registered for GMS, then this is their registered practice too
                if (regType != null
                        && regType == RegistrationType.REGULAR_GMS)
                {
                    Reference orgReferenceCareProvider = csvHelper.createOrganisationReference(orgIdCell);
                    if (patientBuilder.isIdMapped()) {
                        orgReferenceCareProvider = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReferenceCareProvider, csvHelper);
                    }
                    patientBuilder.addCareProvider(orgReferenceCareProvider, orgIdCell);
                }

                //if registration type is not dummy remove the test patient extension
                //the above code never returns the Dummy reg type, so removed this. Just trust the TestPatient field on SRPatient
                /*if (regType != null && regType != RegistrationType.DUMMY) {
                    ExtensionConverter.removeExtension(patientBuilder.getResource(), FhirExtensionUri.PATIENT_IS_TEST_PATIENT);
                }*/
            }
        }

        //save a mapping to allow us to find the active episode for the patient
        if (episodeBuilder.getStatus() == EpisodeOfCare.EpisodeOfCareStatus.ACTIVE) {
            csvHelper.saveInternalId(PATIENT_ID_TO_ACTIVE_EPISODE_ID, patientIdCell.getString(), rowIdCell.getString());
        }

        //we save the episode immediately, since it's complete now, but the patient isn't done yet
        boolean mapIds = !episodeBuilder.isIdMapped();
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), mapIds, episodeBuilder);
        //LOG.debug("Added episode of care " + episodeBuilder.getResourceId() + " for patient " + patientIdCell.getString() + " to resource filer");
    }

    private static RegistrationType mapToFhirRegistrationType(CsvCell regTypeCell) throws Exception {

        //there is some SystmOne legacy data where patients have multiple reg types, generally GMS + the old
        //IOS registrations (e.g. GMS,Contraception). In these cases, carry over GMS.
        List<RegistrationType> types = new ArrayList<>();

        // Main concern is whether patient is GMS, Private or temporary.
        String s = regTypeCell.getString();
        String[] toks = s.split(",");

        for (String tok: toks) {

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

    public static RegistrationStatus convertMedicalRecordStatus(CsvCell statusCell) throws Exception {
        int medicalRecordStatus = statusCell.getInt().intValue();
        switch (medicalRecordStatus) {
            case 0:
                return RegistrationStatus.DEDUCTED_RECORDS_SENT_BACK_TO_FHSA;
            case 1:
                return RegistrationStatus.REGISTERED_RECORD_SENT_FROM_FHSA;
            case 2:
                return RegistrationStatus.REGISTERED_RECORD_RECEIVED_FROM_FHSA;
            case 3:
                return RegistrationStatus.DEDUCTED_RECORDS_RECEIVED_BY_FHSA;
            case 4:
                return RegistrationStatus.DEDUCTED_RECORD_REQUESTED_BY_FHSA;
            default:
                throw new TransformException("Unmapped medical record status " + medicalRecordStatus);
        }
    }

}
