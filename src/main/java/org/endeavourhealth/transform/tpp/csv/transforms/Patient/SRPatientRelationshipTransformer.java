package org.endeavourhealth.transform.tpp.csv.transforms.Patient;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientContactBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.PatientResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientRelationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRPatientRelationshipTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRPatientRelationshipTransformer.class);
    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRPatientRelationshipTransformer.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRPatientRelationship)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(SRPatientRelationship parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {
        CsvCell rowIdCell = parser.getRowIdentifier();
        if ((rowIdCell.isEmpty()) || (!StringUtils.isNumeric(rowIdCell.getString()))) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifier: {} in file : {}", rowIdCell.getString(), parser.getFilePath());
            return;
        }
        CsvCell removeDataCell = parser.getRemovedData();
        if (!removeDataCell.getIntAsBoolean()) {
            return;
        }


        CsvCell IdPatientCell = parser.getIDPatient();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(IdPatientCell, csvHelper, fhirResourceFiler);
        if (IdPatientCell.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }
        PatientContactBuilder contactBuilder = new PatientContactBuilder(patientBuilder);
        contactBuilder.setId(rowIdCell.getString(), rowIdCell);

        CsvCell relationshipWithNameCell = parser.getRelationshipWithName();
        if (!relationshipWithNameCell.isEmpty()) {
            String relationType = relationshipWithNameCell.getString();
            String code = csvHelper.tppRelationtoFhir(relationType);
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(contactBuilder, "relationship");
            codeableConceptBuilder.setText(relationType, relationshipWithNameCell);
            codeableConceptBuilder.setCodingDisplay(relationType,relationshipWithNameCell);
            codeableConceptBuilder.setCodingCode(code);

        }
    }
}
