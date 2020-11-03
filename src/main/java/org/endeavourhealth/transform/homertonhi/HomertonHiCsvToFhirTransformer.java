package org.endeavourhealth.transform.homertonhi;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.ExchangeHelper;
import org.endeavourhealth.transform.common.ExchangePayloadFile;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.homertonhi.schema.*;
import org.endeavourhealth.transform.homertonhi.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class HomertonHiCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(HomertonHiCsvToFhirTransformer.class);

    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String TIME_FORMAT = "HH:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.RFC4180.withHeader();  //TODO check files

    public static void transform(String exchangeBody, FhirResourceFiler fhirResourceFiler, String version) throws Exception {

        List<ExchangePayloadFile> files = ExchangeHelper.parseExchangeBody(exchangeBody);
        UUID serviceId = fhirResourceFiler.getServiceId();
        Service service = DalProvider.factoryServiceDal().getById(serviceId);
        //ExchangeHelper.filterFileTypes(files, service, fhirResourceFiler.getExchangeId());   //TODO: potential file filtering

        LOG.info("Invoking HomertonHi CSV transformer for " + files.size() + " files for service " + service.getName() + " " + service.getId());

        //the files should all be in a directory structure of org folder -> processing ID folder -> CSV files
        String exchangeDirectory = ExchangePayloadFile.validateFilesAreInSameDirectory(files);
        LOG.trace("Transforming HomertonHi CSV content in " + exchangeDirectory);

        HomertonHiCsvHelper csvHelper
                = new HomertonHiCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(), fhirResourceFiler.getExchangeId(), version);

        Map<String, List<ParserI>> parserMap = hashFilesByType(files, exchangeDirectory, csvHelper);

        try {
            // non-patient transforms here

            // process any deletions first by extracting all the deletion hash values to use in each transform

            // process the patient files first, using the Resource caching to collect data from all file before filing
            PersonTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "Person", true), fhirResourceFiler, csvHelper);
            PersonDemographicsTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "Person_Demographics", true), fhirResourceFiler, csvHelper);
            PersonAliasTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "Person_Alias", true), fhirResourceFiler, csvHelper);
            PersonLanguageTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "Person_Language", true), fhirResourceFiler, csvHelper);
            PersonPhoneTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "Person_Phone", true), fhirResourceFiler, csvHelper);
            csvHelper.getPatientCache().filePatientResources(fhirResourceFiler);

            // clinical pre-transformers
            ProcedureCommentTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "Procedure_Comment", true), fhirResourceFiler, csvHelper);

            // subsequent transforms may refer to Patient resources and pre-transforms, so ensure they're all on the DB before continuing
            fhirResourceFiler.waitUntilEverythingIsSaved();

            // clinical transformers
            ProcedureTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "Procedure", true), fhirResourceFiler, csvHelper);
            ConditionTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "Condition", true), fhirResourceFiler, csvHelper);

        } finally {
            //if we had any exception that caused us to bomb out of the transform, we'll have
            //potentially cached resources in the DB, so tidy them up now
            csvHelper.getPatientCache().cleanUpResourceCache();
        }
    }

    private static Map<String, List<ParserI>> hashFilesByType(List<ExchangePayloadFile> files, String exchangeDirectory, HomertonHiCsvHelper csvHelper) throws Exception {

        Map<String, List<ParserI>> ret = new HashMap<>();

        for (ExchangePayloadFile fileObj: files) {

            String file = fileObj.getPath();
            String type = fileObj.getType();  //this is set during sftpReader processing

            ParserI parser = createParser(file, type, csvHelper);

            List<ParserI> list = ret.get(type);
            if (list == null) {
                list = new ArrayList<>();
                ret.put(type, list);
            }
            list.add(parser);
        }

        return ret;
    }

    /**
     * finds parsers for the given file type on any matching files
     */
    private static List<ParserI> getParsers(Map<String, List<ParserI>> parserMap, HomertonHiCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler, String type, boolean removeFromMap) throws Exception {

        //if we had any errors on the previous file, we should bomb out now
        fhirResourceFiler.failIfAnyErrors();

        List<ParserI> ret = null;

        if (removeFromMap) {
            //if removeFromMap is true, it means that this is the last time
            //we'll need the parsers, to remove from the map and allow them to be garbage collected when we're done
            ret = parserMap.remove(type);

        } else {
            ret = parserMap.get(type);
        }

        if (ret == null) {
            ret = new ArrayList<>();
        }

        return ret;
    }

    private static ParserI createParser(String file, String type, HomertonHiCsvHelper csvHelper) throws Exception {

        UUID serviceId = csvHelper.getServiceId();
        UUID systemId = csvHelper.getSystemId();
        UUID exchangeId = csvHelper.getExchangeId();
        String version = csvHelper.getVersion();

        if (type.equalsIgnoreCase("person")) {
            return new Person(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("person_demographics")) {
            return new PersonDemographics(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("person_alias")) {
            return new PersonAlias(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("person_language")) {
            return new PersonLanguage(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("person_phone")) {
            return new PersonPhone(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("procedure")) {
            return new Procedure(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("procedure_comment")) {
            return new ProcedureComment(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("condition")) {
            return new Condition(serviceId, systemId, exchangeId, version, file);
        } else {
            throw new TransformException("Unknown file type [" + type + "]");
        }
    }
}