package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.USERS;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.hl7.fhir.instance.model.HumanName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class USERSTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(USERSTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 AdastraCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(USERS.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((USERS) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(USERS parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        CsvCell userRefCell = parser.getUserRef();
        CsvCell userFullName = parser.getFullName();
        if (userRefCell.isEmpty()) {
            TransformWarnings.log(LOG, parser, "UserRef is blank for PatientId: {}",
                    userFullName.getString());
            return;
        }

        PractitionerBuilder practitionerBuilder = new PractitionerBuilder();
        practitionerBuilder.setId(userRefCell.getString(), userRefCell);

        CsvCell forename = parser.getForename();
        CsvCell surname = parser.getSurname();
        CsvCell fullName = parser.getFullName();

        NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addGiven(forename.getString(), forename);
        nameBuilder.addFamily(surname.getString(), surname);
        nameBuilder.addFullName(fullName.getString(), fullName);

        //TODO - what does this link to / used for? - asked Advanced
        CsvCell providerRefCell = parser.getProviderRef();
        CsvCell providerName = parser.getProviderName();
        CsvCell providerType = parser.getProviderType();

        //if a Doctor
        CsvCell providerGMC = parser.getProviderGMC();
        if (!providerGMC.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMC_NUMBER);
            identifierBuilder.setValue(providerGMC.getString(), providerGMC);
        }

        //if a Nurse
        CsvCell providerNMC = parser.getProviderNMC();
        if (!providerNMC.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_NMC_NUMBER);
            identifierBuilder.setValue(providerNMC.getString(), providerNMC);
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), practitionerBuilder);
    }
}
