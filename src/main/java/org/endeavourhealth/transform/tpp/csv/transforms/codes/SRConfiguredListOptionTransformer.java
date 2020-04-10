package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppConfigListOptionDalI;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRConfiguredListOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class SRConfiguredListOptionTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRConfiguredListOptionTransformer.class);

    /*private static TppConfigListOptionDalI repository = DalProvider.factoryTppConfigListOptionDal();
    public static final String ROW_ID = "RowId";
    public static final String CONFIG_LIST_ID = "ConfigListId";
    public static final String LIST_OPTION = "ListOption";*/

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler) throws Exception {

        AbstractCsvParser parser = parsers.get(SRConfiguredListOption.class);
        if (parser != null) {

            //just bulk load the file into the DB
            String filePath = parser.getFilePath();
            Date dataDate = fhirResourceFiler.getDataDate();
            TppConfigListOptionDalI dal = DalProvider.factoryTppConfigListOptionDal();
            dal.updateLookupTable(filePath, dataDate);
        }
    }

    /*public static void createResource(SRConfiguredListOption parser, FhirResourceFiler fhirResourceFiler, List<TppConfigListOption> mappingsToSave) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell configListId = parser.getConfiguredList();
        CsvCell listOption = parser.getConfiguredListOption();

        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();

        auditWrapper.auditValue(rowId.getPublishedFileId(), rowId.getRecordNumber(), rowId.getColIndex(), ROW_ID);
        auditWrapper.auditValue(configListId.getPublishedFileId(), configListId.getRecordNumber(), configListId.getColIndex(), CONFIG_LIST_ID);
        auditWrapper.auditValue(listOption.getPublishedFileId(), listOption.getRecordNumber(), listOption.getColIndex(), LIST_OPTION);

        TppConfigListOption tppConfigListOption = new TppConfigListOption(rowId.getLong(),
                                    configListId.getLong(),
                                    listOption.getString(),
                                    fhirResourceFiler.getServiceId().toString(),
                                    auditWrapper);

        mappingsToSave.add(tppConfigListOption);

        if (mappingsToSave.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
            List<TppConfigListOption> copy = new ArrayList<>(mappingsToSave);
            mappingsToSave.clear();

            repository.save(fhirResourceFiler.getServiceId(), copy);
        }
    }*/
}
