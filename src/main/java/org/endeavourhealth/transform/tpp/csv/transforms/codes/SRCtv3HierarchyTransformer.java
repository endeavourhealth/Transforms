package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.CTV3HierarchyRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CTV3HierarchyRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRCtv3Hierarchy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRCtv3HierarchyTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRCtv3HierarchyTransformer.class);

    private static CTV3HierarchyRefDalI repository = DalProvider.factoryCTV3HierarchyRefDal();

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler) throws Exception {

        AbstractCsvParser parser = parsers.get(SRCtv3Hierarchy.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRCtv3Hierarchy)parser, fhirResourceFiler);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(SRCtv3Hierarchy parser, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell ctv3ParentReadCode = parser.getCtv3CodeParent();
        CsvCell ctv3ChildReadCode = parser.getCtv3CodeChild();
        CsvCell ctv3ChildLevel = parser.getChildLevel();

        CTV3HierarchyRef ref = new CTV3HierarchyRef(rowId.getLong(),
                ctv3ParentReadCode.getString(),
                ctv3ChildReadCode.getString(),
                ctv3ChildLevel.getInt());

        //save to the DB
        repository.save(ref, fhirResourceFiler.getServiceId());
    }
}
