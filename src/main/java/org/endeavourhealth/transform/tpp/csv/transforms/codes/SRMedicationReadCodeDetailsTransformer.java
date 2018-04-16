package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.MultiLexToCTV3MapDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.MultiLexToCTV3Map;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRMedicationReadCodeDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRMedicationReadCodeDetailsTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRMedicationReadCodeDetailsTransformer.class);

    private static MultiLexToCTV3MapDalI repository = DalProvider.factoryMultiLexToCTV3MapDal();

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler) throws Exception {

        AbstractCsvParser parser = parsers.get(SRMedicationReadCodeDetails.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRMedicationReadCodeDetails)parser, fhirResourceFiler);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(SRMedicationReadCodeDetails parser, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell multiLexProductId = parser.getIDMultiLexProduct();
        CsvCell ctv3ReadCode = parser.getDrugReadCode();
        CsvCell ctv3ReadTerm = parser.getDrugReadCodeDesc();

        MultiLexToCTV3Map mapping = new MultiLexToCTV3Map(rowId.getLong(),
                multiLexProductId.getLong(),
                ctv3ReadCode.getString(),
                ctv3ReadTerm.getString());

        //save to the DB
        repository.save(mapping, fhirResourceFiler.getServiceId());
    }
}
