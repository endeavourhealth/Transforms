package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisCodeDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.TppConfigListOptionDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.TppCtv3LookupDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppCtv3Lookup;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRCtv3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class SRCtv3Transformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRCtv3Transformer.class);

    /*private static TppCtv3LookupDalI repository = DalProvider.factoryTppCtv3LookupDal();
    //public static final String ROW_ID = "RowId";
    public static final String CTV3_CODE = "ctv3Code";
    public static final String CTV3_TEXT = "ctv3Text";*/

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRCtv3.class);
        if (parser != null) {

            TppCtv3LookupDalI dal = DalProvider.factoryTppCtv3LookupDal();
            Date lastUpdated = dal.getMostRecentCtv3CodeDate();
            Date dataDate = fhirResourceFiler.getDataDate();

            //SD-781 - only update the table if our file is newer than what's in there
            if (lastUpdated == null
                    || dataDate.after(lastUpdated)) {

                //bulk load the file into the DB
                String filePath = parser.getFilePath();
                dal.updateLookupTable(filePath, dataDate);
            } else {

                LOG.trace("Not updating Ctv3 code lookup table as table last updated " + lastUpdated + " and current file is from " + dataDate);
            }
        }
    }
}
