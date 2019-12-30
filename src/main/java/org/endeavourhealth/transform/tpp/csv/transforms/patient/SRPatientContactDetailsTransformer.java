package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.ContactPointBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientContactDetails;
import org.hl7.fhir.instance.model.ContactPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRPatientContactDetailsTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRPatientContactDetailsTransformer.class);

    public static final String PHONE_ID_TO_PATIENT_ID = "PhoneIdToPatientId";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRPatientContactDetails.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRPatientContactDetails) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(SRPatientContactDetails parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();

        CsvCell removeDataCell = parser.getRemovedData();
        if (removeDataCell != null && removeDataCell.getIntAsBoolean()) {

            //if removed we won't have a patient ID, so need to look it up
            String patientId = csvHelper.getInternalId(PHONE_ID_TO_PATIENT_ID, rowIdCell.getString());
            if (!Strings.isNullOrEmpty(patientId)) {
                CsvCell dummyPatientCell = CsvCell.factoryDummyWrapper(patientId);

                PatientBuilder patientBuilder = csvHelper.getPatientResourceCache().getOrCreatePatientBuilder(dummyPatientCell, csvHelper);
                if (patientBuilder != null) {
                    ContactPointBuilder.removeExistingContactPointById(patientBuilder, rowIdCell.getString());
                }
            }

            return;
        }

        //ignore any empty numbers
        CsvCell contactNumberCell = parser.getContactNumber();
        if (contactNumberCell.isEmpty()) {
            return;
        }

        CsvCell patientIdCell = parser.getIDPatient();
        PatientBuilder patientBuilder = csvHelper.getPatientResourceCache().getOrCreatePatientBuilder(patientIdCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        //attempt to re-use any existing number with the same ID so we're not constantly changing the order by removing and re-adding
        ContactPointBuilder contactPointBuilder = ContactPointBuilder.findOrCreateForId(patientBuilder, rowIdCell);
        contactPointBuilder.reset();

        /*
        //remove any existing instance of this phone number from the patient
        ContactPointBuilder.removeExistingContactPointById(patientBuilder, rowIdCell.getString());

        ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
        contactPointBuilder.setId(rowIdCell.getString(), contactNumberCell);*/

        ContactPoint.ContactPointUse use = null;
        ContactPoint.ContactPointSystem system = null;

        CsvCell contactTypeCell = parser.getContactType();
        if (!contactTypeCell.isEmpty() && contactTypeCell.getLong() > 0) {
            TppMappingRef mapping = csvHelper.lookUpTppMappingRef(contactTypeCell);
            if (mapping != null) {
                String term = mapping.getMappedTerm();
                if (term.equalsIgnoreCase("Home")) {
                    use = ContactPoint.ContactPointUse.HOME;
                    system = ContactPoint.ContactPointSystem.PHONE;
                } else if (term.equalsIgnoreCase("Work")) {
                    use = ContactPoint.ContactPointUse.WORK;
                    system = ContactPoint.ContactPointSystem.PHONE;
                } else if (term.equalsIgnoreCase("Emergency Contact")) {
                    use = ContactPoint.ContactPointUse.OLD;
                    system = ContactPoint.ContactPointSystem.OTHER;
                } else if (term.equalsIgnoreCase("Answering Machine")) {
                    use = ContactPoint.ContactPointUse.OLD;
                    system = ContactPoint.ContactPointSystem.OTHER;
                } else if (term.equalsIgnoreCase("Pager")) {
                    use = ContactPoint.ContactPointUse.WORK;
                    system = ContactPoint.ContactPointSystem.PAGER;
                } else if (term.equalsIgnoreCase("Mobile")) {
                    use = ContactPoint.ContactPointUse.MOBILE;
                    system = ContactPoint.ContactPointSystem.PHONE;
                } else if (term.equalsIgnoreCase("Alternate")) {
                    use = ContactPoint.ContactPointUse.OLD;
                    system = ContactPoint.ContactPointSystem.PHONE;
                } else if (term.equalsIgnoreCase("Temporary")) {
                    use = ContactPoint.ContactPointUse.TEMP;
                    system = ContactPoint.ContactPointSystem.PHONE;
                } else if (term.equalsIgnoreCase("Skype")) {
                    use = ContactPoint.ContactPointUse.HOME;
                    system = ContactPoint.ContactPointSystem.EMAIL;
                } else {
                    //not had any warnings in months, so will assume the above is now 100% and can replace with an exception
                    throw new Exception("Unexpected ContactDetails type [" + term + "]");
                    //TransformWarnings.log(LOG, parser, "Unable to convert contact type {} to ContactPointUse", term);
                }
            }
        }

        contactPointBuilder.setValue(contactNumberCell.getString(), contactNumberCell);

        if (use != null) {
            contactPointBuilder.setUse(use, contactNumberCell);
        }
        if (system != null) {
            contactPointBuilder.setSystem(system, contactNumberCell);
        }

        // boolean mapids = !patientBuilder.isIdMapped();
        // fhirResourceFiler.savePatientResource(parser.getCurrentState(), mapids, patientBuilder);
        // Filing done by cache
    }
}
