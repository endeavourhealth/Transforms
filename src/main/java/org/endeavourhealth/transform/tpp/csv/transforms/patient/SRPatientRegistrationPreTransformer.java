package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientRegistration;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SRPatientRegistrationPreTransformer {
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

    /**
     * pre-transformer to save the patient ID -> active registration mapping and set the care provider on the FHIR patient
     * DOES NOT do anything with EpisodeOfCare resources
     */
    public static void createResource(SRPatientRegistration parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();

        //if a deleted registration, do nothing
        CsvCell removeDataCell = parser.getRemovedData();
        if (removeDataCell != null && removeDataCell.getIntAsBoolean()) {
            return;
        }

        //if the registration doesn't have a reg type, do nothing
        CsvCell regTypeCell = parser.getRegistrationStatus();
        if (regTypeCell.isEmpty()) {
            return;
        }
        RegistrationType regType = SRPatientRegistrationTransformer.mapToFhirRegistrationType(regTypeCell);

        //if the registration is ended, do nothing
        CsvCell regEndDateCell = parser.getDateDeRegistration();
        if (!regEndDateCell.isEmpty()) {
            Date d = regEndDateCell.getDate();
            if (!d.before(new Date())) {
                return;
            }
        }

        //the TPP feed supplies details of registrations elsewhere, which should not be saved within the scope
        //of the publisher, as it ends up creating a lot of confusion as episodes of care are expected to be ABOUT
        //the publisher service and not somewhere else
        CsvCell orgIdCell = parser.getIDOrganisationVisibleTo();
        if (!SRPatientRegistrationTransformer.shouldSaveEpisode(parser.getIDOrganisation(), orgIdCell)) {
            return;
        }

        CsvCell patientIdCell = parser.getIDPatient();

        //save a mapping to allow us to find the active episode for the patient
        csvHelper.saveInternalId(PATIENT_ID_TO_ACTIVE_EPISODE_ID, patientIdCell.getString(), rowIdCell.getString());

        //if this registration is an active GMS registration, then it's telling us the current registered GP practice,
        //so this needs setting in the patient careProvider (i.e. current registered practice)
        if (regType == RegistrationType.REGULAR_GMS) {

            PatientBuilder patientBuilder = csvHelper.getPatientResourceCache().borrowPatientBuilder(patientIdCell, csvHelper, fhirResourceFiler);
            if (patientBuilder != null) {

                Reference orgReferenceCareProvider = csvHelper.createOrganisationReference(orgIdCell);
                if (patientBuilder.isIdMapped()) {
                    orgReferenceCareProvider = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReferenceCareProvider, csvHelper);
                }

                patientBuilder.clearCareProvider();
                patientBuilder.addCareProvider(orgReferenceCareProvider, orgIdCell);

                csvHelper.getPatientResourceCache().returnPatientBuilder(patientIdCell, patientBuilder);
            }
        }
    }
}
