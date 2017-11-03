package org.endeavourhealth.transform.emis.csv.transforms.agreements;

import com.fasterxml.jackson.databind.JsonNode;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.emis.csv.schema.agreements.SharingOrganisation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SharingOrganisationTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SharingOrganisationTransformer.class);

    private static Boolean allowDisabledOrganisations = null;

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        SharingOrganisation parser = (SharingOrganisation)parsers.get(SharingOrganisation.class);

        //we don't actually generate any FHIR resources from this file, but check to make sure
        //that the sharing agreement hasn't been disabled. If it has, then we throw an exception
        //and fail the transform. This will mean that all the patient deletes aren't processed
        //and no further messages for this organisation will be processed either. When Emis
        //fix the problem then this will need removing to allow those messages to be processed.
        parser.nextRecord();

        boolean isDisabled = parser.getDisabled();
        if (isDisabled) {
            if (!getAllowDisabledOrganisations()) {
                throw new TransformException("Not processing Exchange because org disabled in sharing agreements file");
            }
        }
    }

    private static boolean getAllowDisabledOrganisations() {
        if (allowDisabledOrganisations == null) {
            boolean b;
            try {
                JsonNode ex = ConfigManager.getConfigurationAsJson("emis", "queuereader");
                b = ex.get("process_disabled").asBoolean();
            } catch (Exception var4) {
                b = false;
            }

            allowDisabledOrganisations = new Boolean(b);
            LOG.info("Allowing Disabled Emis Organisations = " + allowDisabledOrganisations);
        }
        return allowDisabledOrganisations.booleanValue();
    }
}
