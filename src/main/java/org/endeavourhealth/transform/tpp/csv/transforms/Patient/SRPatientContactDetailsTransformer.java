package org.endeavourhealth.transform.tpp.csv.transforms.Patient;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.ContactPointBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.PatientResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientContactDetails;
import org.hl7.fhir.instance.model.ContactPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRPatientContactDetailsTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRPatientContactDetailsTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRPatientContactDetailsTransformer.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRPatientContactDetails)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(SRPatientContactDetails parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();
        if ((rowIdCell.isEmpty()) || (!StringUtils.isNumeric(rowIdCell.getString()))) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifier: {} in file : {}", rowIdCell.getString(), parser.getFilePath());
            return;
        }
        CsvCell removeDataCell = parser.getRemovedData();
        if (removeDataCell.getIntAsBoolean()) {
            return;
        }


        CsvCell IdPatientCell = parser.getIDPatient();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(IdPatientCell, csvHelper, fhirResourceFiler);
        if (IdPatientCell.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }


        ContactPoint.ContactPointUse use = null;
        CsvCell contactTypeCell = parser.getContactType();
        if (!contactTypeCell.isEmpty() && contactTypeCell.getLong() > 0) {
            SnomedCode snomedCode = TerminologyService.translateCtv3ToSnomed(contactTypeCell.getString());
            try {
                use = ContactPoint.ContactPointUse.fromCode(snomedCode.getConceptCode());
            } catch (Exception ex) {
                TransformWarnings.log(LOG, parser, "Unrecognized contact type {} in file {}",
                        contactTypeCell.getString(), parser.getFilePath());
                return;
            }
        }

        CsvCell contactNumberCell = parser.getContactNumber();
        if (!contactNumberCell.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setId(rowIdCell.getString(), contactNumberCell);
            if (use != null) {
                contactPointBuilder.setUse(use, contactNumberCell);
            }
            contactPointBuilder.setValue(contactNumberCell.getString(), contactNumberCell);
        }
    }
}
