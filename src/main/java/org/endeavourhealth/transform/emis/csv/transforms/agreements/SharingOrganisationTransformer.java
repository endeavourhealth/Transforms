package org.endeavourhealth.transform.emis.csv.transforms.agreements;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.emis.csv.schema.agreements.SharingOrganisation;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Period;
import org.hl7.fhir.instance.model.Practitioner;

import java.util.Date;
import java.util.Map;

public class SharingOrganisationTransformer {

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
            //need to remove this so we let the data through again
            //throw new TransformException("Not processing Exchange because org disabled in sharing agreements file");
        }
    }

}
