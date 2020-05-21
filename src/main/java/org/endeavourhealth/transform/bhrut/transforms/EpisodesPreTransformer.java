package org.endeavourhealth.transform.bhrut.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.Episodes;
import org.endeavourhealth.transform.bhrut.schema.Spells;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EpisodesPreTransformer {


    private static final Logger LOG = LoggerFactory.getLogger(EpisodesPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Episodes.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    Episodes episodesParser = (Episodes) parser;

                    if (!episodesParser.getLinestatus().getString().equalsIgnoreCase("delete")) {
                        cacheResources(episodesParser, fhirResourceFiler, csvHelper, version);
                    }
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void cacheResources(Episodes parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BhrutCsvHelper csvHelper,
                                      String version) throws Exception {

        if (!parser.getEpisodeConsultantCode().isEmpty()) {
            String name = csvHelper.getStaffCache().getNameForCcode(parser.getEpisodeConsultantCode().getString());
            if (Strings.isNullOrEmpty(name)) {
                csvHelper.getStaffCache().addConsultantCode(parser.getEpisodeConsultantCode(), parser.getEpisodeConsultant());
            }
        }

        if (!parser.getAdmissionHospitalCode().isEmpty()) {
            String name = csvHelper.getOrgCache().getNameForOrgCode(parser.getAdmissionHospitalCode().getString());
            if (Strings.isNullOrEmpty(name)) {
                csvHelper.getOrgCache().addOrgCode(parser.getAdmissionHospitalCode(), parser.getAdmissionHospitalName());
            }
        }
    }
}
