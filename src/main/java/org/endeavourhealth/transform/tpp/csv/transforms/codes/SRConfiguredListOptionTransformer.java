package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.TppConfigListOptionDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppConfigListOption;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRConfiguredListOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SRConfiguredListOptionTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRConfiguredListOptionTransformer.class);

    private static TppConfigListOptionDalI repository = DalProvider.factoryTppConfigListOptionDal();
    public static final String ROW_ID = "RowId";
    public static final String CONFIG_LIST_ID = "ConfigListId";
    public static final String LIST_OPTION = "ListOption";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler) throws Exception {

        List<TppConfigListOption> mappingsToSave = new ArrayList<>();

        AbstractCsvParser parser = parsers.get(SRConfiguredListOption.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRConfiguredListOption) parser, fhirResourceFiler, mappingsToSave);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //and save any still pending
        if (!mappingsToSave.isEmpty()) {
            repository.save(fhirResourceFiler.getServiceId(), mappingsToSave);
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(SRConfiguredListOption parser, FhirResourceFiler fhirResourceFiler, List<TppConfigListOption> mappingsToSave) throws Exception {

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
    }
}
