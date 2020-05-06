package org.endeavourhealth.transform.bhrut.transforms;

import com.sun.deploy.net.proxy.pac.PACFunctions;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.bhrut.schema.PMI;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PMITransformer {


    private static final Logger LOG = LoggerFactory.getLogger(PMITransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BhrutCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(PMI.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResources((PMI) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResources(PMI parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BhrutCsvHelper csvHelper,
                                       String version) throws Exception {

        PatientBuilder patientBuilder = new PatientBuilder();
        patientBuilder.setId(parser.getID().toString());

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, patientBuilder);
    }
}
