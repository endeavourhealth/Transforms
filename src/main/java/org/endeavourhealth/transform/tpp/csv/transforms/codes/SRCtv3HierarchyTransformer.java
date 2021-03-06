package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppCtv3HierarchyRefDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRCtv3Hierarchy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class SRCtv3HierarchyTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRCtv3HierarchyTransformer.class);

    //private static TppCtv3HierarchyRefDalI repository = DalProvider.factoryTppCtv3HierarchyRefDal();

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRCtv3Hierarchy.class);
        if (parser != null) {

            //just bulk load the file into the DB
            String filePath = parser.getFilePath();
            Date dataDate = fhirResourceFiler.getDataDate();
            TppCtv3HierarchyRefDalI dal = DalProvider.factoryTppCtv3HierarchyRefDal();
            dal.updateHierarchyTable(filePath, dataDate);
        }
    }

    /*public static void processRecord(SRCtv3Hierarchy parser, TppCsvHelper csvHelper, List<TppCtv3HierarchyRef> mappingsToSave) throws Exception {

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

        TppCtv3HierarchyRef mapping = new TppCtv3HierarchyRef(rowId.getLong(),
                ctv3ParentReadCode.getString(),
                ctv3ChildReadCode.getString(),
                ctv3ChildLevel.getInt());

        mappingsToSave.add(mapping);

        if (mappingsToSave.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
            List<TppCtv3HierarchyRef> copy = new ArrayList<>(mappingsToSave);
            mappingsToSave.clear();
            csvHelper.submitToThreadPool(new Task(copy));
        }
    }

    static class Task implements Callable {

        private List<TppCtv3HierarchyRef> mappingsToSave;

        public Task(List<TppCtv3HierarchyRef> mappingsToSave) {

            this.mappingsToSave = mappingsToSave;
        }

        @Override
        public Object call() throws Exception {

            try {
                //save to the DB
                repository.save(mappingsToSave);

            } catch (Throwable t) {
                String msg = "Error saving CTV3 hierarchy records for row IDs ";
                for (TppCtv3HierarchyRef mapping: mappingsToSave) {
                    msg += mapping.getRowId();
                    msg += ", ";
                }

                LOG.error(msg, t);
                throw new TransformException(msg, t);
            }

            return null;
        }
    }*/
}
