package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppCtv3HierarchyRefDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppCtv3HierarchyRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
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
        if (rowId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifier: {} in file : {}",
                    rowId.getString(), parser.getFilePath());
            return;
        }
        CsvCell ctv3ParentReadCode = parser.getCtv3CodeParent();
        if (ctv3ParentReadCode.isEmpty()) {
            TransformWarnings.log(LOG, parser, "ERROR: Parent Read code missing: {} for rowId{} in file : {}",
                    ctv3ParentReadCode.getString(),rowId.getString(), parser.getFilePath());
            return;
        }
        CsvCell ctv3ChildReadCode = parser.getCtv3CodeChild();
        if (ctv3ChildReadCode.isEmpty()) {
            TransformWarnings.log(LOG, parser, "ERROR: Child Read code missing: {} for rowId{} in file : {}",
                    ctv3ChildReadCode.getString(),rowId.getString(), parser.getFilePath());
            return;
        }
        CsvCell ctv3ChildLevel = parser.getChildLevel();
        if (ctv3ChildLevel.isEmpty()) {
            TransformWarnings.log(LOG, parser, "ERROR: Child level Read code missing: {} for rowId{} in file : {}",
                    ctv3ChildLevel.getString(),rowId.getString(), parser.getFilePath());
            return;
        }


        TppCtv3HierarchyRef ref = new TppCtv3HierarchyRef(rowId.getLong(),
                ctv3ParentReadCode.getString(),
                ctv3ChildReadCode.getString(),
                ctv3ChildLevel.getInt());

        //save to the DB
        repository.save(ref);
    }
}
