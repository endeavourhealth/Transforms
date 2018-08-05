package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientRegistration;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRPatientRegistrationTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRPatientRegistrationTransformer.class);

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
            boolean mapIds = !episodeBuilder.isIdMapped();
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), mapIds, episodeBuilder);
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

        CsvCell medicalRecordStatusCell = csvHelper.getAndRemoveMedicalRecordStatus(patientIdCell);
        if (medicalRecordStatusCell != null && !medicalRecordStatusCell.isEmpty()) {
            String medicalRecordStatus = convertMedicalRecordStatus(medicalRecordStatusCell.getInt());
            episodeBuilder.setMedicalRecordStatus(medicalRecordStatus, medicalRecordStatusCell);
        }

        CsvCell regTypeCell = parser.getRegistrationStatus();
        RegistrationType regType = null;
        if (!regTypeCell.isEmpty()) {
            regType = mapToFhirRegistrationType(regTypeCell);
            episodeBuilder.setRegistrationType(regType, regEndDateCell);
        }

        CsvCell orgIdCell = parser.getIDOrganisation();
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
            }
        }

        //we save the episode immediately, since it's complete now, but the patient isn't done yet
        boolean mapIds = !episodeBuilder.isIdMapped();
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), mapIds, episodeBuilder);
    }

    private static RegistrationType mapToFhirRegistrationType(CsvCell regTypeCell) throws Exception {
        // Fold to upper for easy comparison. Main concern is whether
        // patient is GMS, Private or temporary.
        String target = regTypeCell.getString().toUpperCase();

        if (target.equals("GMS")) {
            return RegistrationType.REGULAR_GMS;
        } else if (target.equals("IMMEDIATELY NECESSARY")) {
            return RegistrationType.IMMEDIATELY_NECESSARY;
        } else if (target.equals("PRIVATE")) {
            return RegistrationType.PRIVATE;
        } else if (target.equals("TEMPORARY")) {
            return RegistrationType.TEMPORARY;
        } else if (target.equals("APPLIED")) {
            return RegistrationType.PRE_REGISTRATION;

        } else {
            throw new TransformException("Unmapped registration type " + target);
            //return RegistrationType.OTHER;
        }
    }

    public static String convertMedicalRecordStatus(Integer medicalRecordStatus) throws Exception {
        switch (medicalRecordStatus) {
            case 0:
                return "No medical records";
            case 1:
                return "Medical records are on the way";
            case 2:
                return "Medical records here";
            case 3:
                return "Medical records sent";
            case 4:
                return "Medical records need to be sent";
            default:
                throw new TransformException("Unmapped medical record status " + medicalRecordStatus);
        }
    }
}
