package org.endeavourhealth.transform.emis.csv.transforms.agreements;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.agreements.SharingOrganisation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SharingOrganisationTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SharingOrganisationTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        SharingOrganisation parser = (SharingOrganisation)parsers.get(SharingOrganisation.class);

        //we don't actually generate any FHIR resources from this file, but check to make sure
        //that the sharing agreement hasn't been disabled. If it has, then we throw an exception
        //and fail the transform. This will mean that all the patient deletes aren't processed
        //and no further messages for this organisation will be processed either. When Emis
        //fix the problem then this will need removing to allow those messages to be processed.
        parser.nextRecord();

        CsvCell disabled = parser.getDisabled();
        csvHelper.setSharingAgreementDisabled(disabled.getBoolean());

        //if disabled, then we only want to process the extract if we've explicitly set up config to allow it
        if (disabled.getBoolean()) {

            //if we've already decided we don't want to process any patient data, then we don't care what the state
            //of the sharing agreement is
            if (csvHelper.isProcessPatientData()
                    && !csvHelper.allowedToProcessedDisabledExtract()) {

                throw new TransformException("Not processing Exchange because org disabled in sharing agreements file");
            }
        }


    }


}
