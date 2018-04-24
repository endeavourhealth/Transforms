package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppCtv3HierarchyRefDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppCtv3HierarchyRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRCtv3Hierarchy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRCtv3HierarchyTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRCtv3HierarchyTransformer.class);

    private static TppCtv3HierarchyRefDalI repository = DalProvider.factoryTppCtv3HierarchyRefDal();

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

        TppCtv3HierarchyRef ref = new TppCtv3HierarchyRef(rowId.getLong(),
                ctv3ParentReadCode.getString(),
                ctv3ChildReadCode.getString(),
                ctv3ChildLevel.getInt());

        //save to the DB
        repository.save(ref);
    }
}
