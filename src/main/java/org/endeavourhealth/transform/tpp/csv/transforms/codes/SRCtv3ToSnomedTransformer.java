package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppCtv3SnomedRefDalI;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRCtv3ToSnomed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class SRCtv3ToSnomedTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRCtv3ToSnomedTransformer.class);

    //private static TppCtv3HierarchyRefDalI repository = DalProvider.factoryTppCtv3HierarchyRefDal();

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRCtv3ToSnomed.class);
        if (parser != null) {

            //just bulk load the file into the DB
            String filePath = parser.getFilePath();
            Date dataDate = fhirResourceFiler.getDataDate();
            TppCtv3SnomedRefDalI dal = DalProvider.factoryTppCtv3SnomedRefDal();
            dal.updateSnomedTable(filePath, dataDate);
        }
    }
}
