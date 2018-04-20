package org.endeavourhealth.transform.tpp.csv.transforms.Patient;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
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

        AbstractCsvParser parser = parsers.get(SRPatientRegistrationTransformer.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRPatientRegistration)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
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
        if (removeDataCell.getIntAsBoolean()) {
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

        CsvCell IdPatientCell = parser.getIDPatient();

        if (IdPatientCell.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(IdPatientCell, csvHelper, fhirResourceFiler);
        Reference organizationReference = null;
        EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder();
        CsvCell orgIdCell = parser.getIDOrganisationRegisteredAt();
        if (!orgIdCell.isEmpty()) {
            OrganizationBuilder organizationBuilder = new OrganizationBuilder();
            organizationBuilder.setId(orgIdCell.getString());
            organizationReference = csvHelper.createOrganisationReference(orgIdCell);
            patientBuilder.addCareProvider(organizationReference);
            episodeBuilder.setManagingOrganisation(organizationReference, orgIdCell);
        }

        CsvCell regStartDateCell = parser.getDateRegistration();
        if (!regStartDateCell.isEmpty()) {
            episodeBuilder.setRegistrationStartDate(regStartDateCell.getDate(), regStartDateCell);
        }
        CsvCell regEndDateCell = parser.getDateDeRegistration();
        if (!regEndDateCell.isEmpty()) {
            episodeBuilder.setRegistrationEndDate(regEndDateCell.getDate(), regEndDateCell);
        }

        CsvCell regTypeCell = parser.getRegistrationStatus();
        if (!regTypeCell.isEmpty()) {
            episodeBuilder.setRegistrationType(mapToFhirRegistrationType(regTypeCell));
        }

        String medicalRecordStatus = csvHelper.getAndRemoveMedicalRecordStatus(IdPatientCell);
        if (medicalRecordStatus != null) {
            //TODO - need to carry through the audit of where this status came from, in whatever file it was originally read
            episodeBuilder.setMedicalRecordStatus(medicalRecordStatus, null);
        }
    }

    private static RegistrationType mapToFhirRegistrationType (CsvCell regTypeCell) {
        // Fold to upper for easy comparison. Main concern is whether
        // patient is GMS, Private or temporary.
        String target = regTypeCell.getString().toUpperCase();

        if (target.contains("GMS")) {
            return RegistrationType.REGULAR_GMS;
        } else if (target.contains("IMMEDIATELY NECESSARY")) {
            return RegistrationType.IMMEDIATELY_NECESSARY; // Historical
        } else if(target.contains("PRIVATE")) {
            return RegistrationType.PRIVATE;
        } else if(target.contains("TEMPORARY")) {
            return RegistrationType.TEMPORARY;
        } else {
            return RegistrationType.OTHER;
        }


    }
}
