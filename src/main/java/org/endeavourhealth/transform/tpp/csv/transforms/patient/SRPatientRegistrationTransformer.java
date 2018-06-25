package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.EpisodeOfCareResourceCache;
import org.endeavourhealth.transform.tpp.cache.PatientResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientRegistration;
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
    }

    public static void createResource(SRPatientRegistration parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();
        if ((rowIdCell.isEmpty()) || (!StringUtils.isNumeric(rowIdCell.getString()))) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifier: {} in file : {}",
                    rowIdCell.getString(), parser.getFilePath());
            return;
        }
        CsvCell removeDataCell = parser.getRemovedData();
        if ((removeDataCell != null) && !removeDataCell.isEmpty() && removeDataCell.getIntAsBoolean()) {
            org.hl7.fhir.instance.model.EpisodeOfCare episode
                    = (org.hl7.fhir.instance.model.EpisodeOfCare) csvHelper.retrieveResource(rowIdCell.getString(),
                    ResourceType.EpisodeOfCare,
                    fhirResourceFiler);

            if (episode != null) {
                EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(episode);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), episodeOfCareBuilder);
            }
            return;
        }

        CsvCell idPatient = parser.getIDPatient();

        if (idPatient.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        PatientBuilder patientBuilder = PatientResourceCache.getOrCreatePatientBuilder(idPatient, csvHelper, fhirResourceFiler);

        EpisodeOfCareBuilder episodeBuilder = EpisodeOfCareResourceCache.getOrCreateEpisodeOfCareBuilder(idPatient,
                csvHelper, fhirResourceFiler);
        episodeBuilder.setId(idPatient.getString());  //use the patient GUID as the ID for the episode
        Reference patientReference = csvHelper.createPatientReference(idPatient);
        episodeBuilder.setPatient(patientReference, idPatient);
        CsvCell orgIdCell = parser.getIDOrganisationRegisteredAt();
        if (!orgIdCell.isEmpty()) {
            OrganizationBuilder organizationBuilder = new OrganizationBuilder();
            organizationBuilder.setId(orgIdCell.getString());
            Reference organizationReference = csvHelper.createOrganisationReference(orgIdCell);
            if (patientBuilder.isIdMapped()) {
                organizationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference,fhirResourceFiler);
            }
            patientBuilder.addCareProvider(organizationReference);
            patientBuilder.setManagingOrganisation(organizationReference, orgIdCell);
            episodeBuilder.setManagingOrganisation(organizationReference, orgIdCell);
        }

        CsvCell regStartDateCell = parser.getDateRegistration();
        if (!regStartDateCell.isEmpty()) {
            episodeBuilder.setRegistrationStartDate(regStartDateCell.getDate(), regStartDateCell);
        }
        CsvCell regEndDateCell = parser.getDateDeRegistration();
        if (!regEndDateCell.isEmpty()) {
            episodeBuilder.setRegistrationEndDate(regEndDateCell.getDate(), regEndDateCell);
            patientBuilder.setActive(false, regEndDateCell);
        } else if (!regStartDateCell.isEmpty()) {
            patientBuilder.setActive(true, regStartDateCell);
        }

        CsvCell regTypeCell = parser.getRegistrationStatus();
        if (!regTypeCell.isEmpty()) {
            episodeBuilder.setRegistrationType(mapToFhirRegistrationType(regTypeCell));
        }

        CsvCell medicalRecordStatusCell = csvHelper.getAndRemoveMedicalRecordStatus(idPatient);
        if (medicalRecordStatusCell != null && !medicalRecordStatusCell.isEmpty()) {
            String medicalRecordStatus = convertMedicalRecordStatus(medicalRecordStatusCell.getInt());
            episodeBuilder.setMedicalRecordStatus(medicalRecordStatus, medicalRecordStatusCell);
        }
        // fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientBuilder, episodeBuilder);
        // Moved to PatientResourceCache
    }

    private static RegistrationType mapToFhirRegistrationType(CsvCell regTypeCell) {
        // Fold to upper for easy comparison. Main concern is whether
        // patient is GMS, Private or temporary.
        String target = regTypeCell.getString().toUpperCase();

        if (target.contains("GMS")) {
            return RegistrationType.REGULAR_GMS;
        } else if (target.contains("IMMEDIATELY NECESSARY")) {
            return RegistrationType.IMMEDIATELY_NECESSARY; // Historical
        } else if (target.contains("PRIVATE")) {
            return RegistrationType.PRIVATE;
        } else if (target.contains("TEMPORARY")) {
            return RegistrationType.TEMPORARY;
        } else {
            return RegistrationType.OTHER;
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
