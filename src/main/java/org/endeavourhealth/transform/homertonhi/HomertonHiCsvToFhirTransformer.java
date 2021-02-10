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
    public static final String TIME_FORMAT = "'T'HH:mm:ss";   //note: 'T' prefixed as Homerton date times are DateFormat yyyy-MM-dd'T'HH:mm:ss
    public static final CSVFormat CSV_FORMAT = CSVFormat.RFC4180.withHeader();

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

            // process any deletions first by using the deletion hash value lookups to use in each transform
            // note ordering of clinical deletions first, then patients
            ConditionTransformer.delete(getParsers(parserMap, csvHelper, fhirResourceFiler, "condition_delete", true), fhirResourceFiler, csvHelper);
            ProcedureTransformer.delete(getParsers(parserMap, csvHelper, fhirResourceFiler, "procedure_delete", true), fhirResourceFiler, csvHelper);
            PersonAliasTransformer.delete(getParsers(parserMap, csvHelper, fhirResourceFiler, "person_alias_delete", true), fhirResourceFiler, csvHelper);
            PersonPhoneTransformer.delete(getParsers(parserMap, csvHelper, fhirResourceFiler, "person_phone_delete", true), fhirResourceFiler, csvHelper);
            PersonAddressTransformer.delete(getParsers(parserMap, csvHelper, fhirResourceFiler, "person_address_delete", true), fhirResourceFiler, csvHelper);
            PersonTransformer.delete(getParsers(parserMap, csvHelper, fhirResourceFiler, "person_delete", true), fhirResourceFiler, csvHelper);

            // process the patient files first, using the Resource caching to collect data from all files before filing
            PersonTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "person", true), fhirResourceFiler, csvHelper);
            PersonAddressTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "person_address", true), fhirResourceFiler, csvHelper);
            PersonDemographicsTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "person_demographics", true), fhirResourceFiler, csvHelper);
            PersonAliasTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "person_alias", true), fhirResourceFiler, csvHelper);
            PersonLanguageTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "person_language", true), fhirResourceFiler, csvHelper);
            PersonPhoneTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "person_phone", true), fhirResourceFiler, csvHelper);
            csvHelper.getPatientCache().filePatientResources(fhirResourceFiler);

            // clinical pre-transformers
            ProcedureCommentTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "procedure_comment", true), fhirResourceFiler, csvHelper);

            // subsequent transforms may refer to Patient resources and pre-transforms, so ensure they're all on the DB before continuing
            fhirResourceFiler.waitUntilEverythingIsSaved();

            // clinical transformers
            ProcedureTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "procedure", true), fhirResourceFiler, csvHelper);
            ConditionTransformer.transform(getParsers(parserMap, csvHelper, fhirResourceFiler, "condition", true), fhirResourceFiler, csvHelper);

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

        //TODO: handle those included files which will not be transformed,
        // i.e. procedure_comment_delete, person_language_delete, person_demographics_delete

        if (type.equalsIgnoreCase("person")) {
            return new Person(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("person_delete")) {
            return new PersonDelete(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("person_demographics")) {
            return new PersonDemographics(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("person_alias")) {
            return new PersonAlias(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("person_alias_delete")) {
            return new PersonAliasDelete(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("person_address")) {
            return new PersonAddress(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("person_address_delete")) {
            return new PersonAddressDelete(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("person_language")) {
            return new PersonLanguage(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("person_phone")) {
            return new PersonPhone(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("person_phone_delete")) {
            return new PersonPhoneDelete(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("procedure")) {
            return new Procedure(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("procedure_delete")) {
            return new ProcedureDelete(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("procedure_comment")) {
            return new ProcedureComment(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("condition")) {
            return new Condition(serviceId, systemId, exchangeId, version, file);
        } else if (type.equalsIgnoreCase("condition_delete")) {
            return new ConditionDelete(serviceId, systemId, exchangeId, version, file);
        } else {
            throw new TransformException("Unknown file type [" + type + "]");
        }
    }
}
