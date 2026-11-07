package org.endeavourhealth.transform.nems.birthNotification.admin;

import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.nems.NemsXmlHelper;
import org.endeavourhealth.transform.nems.schema.v1.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBElement;

import static org.endeavourhealth.transform.nems.DataCache.*;

public class PractitionerTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PractitionerTransformer.class);


    public static void transform(BundleType birthNotification, FhirResourceFiler processor, NemsXmlHelper xmlHelper) throws Exception {

        for (EntryType entry: birthNotification.getEntry()) {
            if (entry.getResource().getPractitioner() != null){
                Practitioner practitioner = new Practitioner();
                practitioner.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PRACTITIONER));
                practitioner.setId(entry.getResource().getPractitioner().getId().getValue());

                PractitionerBuilder practitionerBuilder = new PractitionerBuilder(practitioner);
                NameType person = entry.getResource().getPractitioner().getName();
                createName(practitionerBuilder, person);

                practitioners.add(practitionerBuilder.getResourceId());

                processor.saveAdminResource(null, practitionerBuilder);
            }
        }
    }

    private static void createName(PractitionerBuilder patientBuilder, NameType practitionerName) throws Exception {
        FamilyType familyName = null;
        GivenType givenName = null;

        for(int i = 0; i < practitionerName.getContent().size(); i++){
            if(practitionerName.getContent().get(i) instanceof JAXBElement<?>) {
                JAXBElement<?> element = (JAXBElement<?>) practitionerName.getContent().get(i);
                if (element.getValue() instanceof FamilyType) {
                    familyName = (FamilyType) element.getValue();
                } else if (element.getValue() instanceof GivenType) {
                    givenName = (GivenType) element.getValue();
                }
            }
        }


        if (patientBuilder.getNames().isEmpty()) {
            NameBuilder nameBuilder = new NameBuilder(patientBuilder);
            nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
            String nameText = "";
            if(null != givenName) {
                nameBuilder.addGiven(givenName.getValue());
                nameText = givenName.getValue();
            }
            if(null != familyName) {
                nameBuilder.addFamily(familyName.getValue());
                nameText = nameText + " " + familyName.getValue();
            }

            nameBuilder.setText(nameText);
        }
    }


}
