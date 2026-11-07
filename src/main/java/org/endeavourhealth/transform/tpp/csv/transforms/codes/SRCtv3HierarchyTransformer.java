package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppCtv3HierarchyRefDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.TppCtv3LookupDalI;
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

            TppCtv3HierarchyRefDalI dal = DalProvider.factoryTppCtv3HierarchyRefDal();
            Date lastUpdated = dal.getMostRecentCtv3HierarchyDate();
            Date dataDate = fhirResourceFiler.getDataDate();

            //SD-781 - only update the table if our file is newer than what's in there
            if (lastUpdated == null
                    || dataDate.after(lastUpdated)) {

                //bulk load the file into the DB
                String filePath = parser.getFilePath();
                dal.updateHierarchyTable(filePath, dataDate);
            } else {

                LOG.trace("Not updating Ctv3 hierarchy table as table last updated " + lastUpdated + " and current file is from " + dataDate);
            }
        }
    }
}
