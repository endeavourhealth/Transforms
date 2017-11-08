package org.endeavourhealth.transform.adastra.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper;
import org.endeavourhealth.transform.common.XmlDateHelper;
import org.hl7.fhir.instance.model.*;

import javax.xml.datatype.XMLGregorianCalendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class FlagTransform {

    public static void transform(AdastraCaseDataExport.SpecialNote specialNote, String consultationId, List<Resource> resources) {
        Flag fhirFlag = new Flag();

        //TODO add Flag and change the meta to flag
        fhirFlag.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_OBSERVATION));


        //Need to check if this needs to be consistent
        String observationGuid = UUID.randomUUID().toString();

        AdastraHelper.setUniqueId(fhirFlag, observationGuid);

        fhirFlag.setSubject(AdastraHelper.createPatientReference());

        Date reviewDate = XmlDateHelper.convertDate(specialNote.getReviewDate());
        Period fhirPeriod = PeriodHelper.createPeriod(null, reviewDate);
        fhirFlag.setPeriod(fhirPeriod);

        CodeableConcept codeableConcept = new CodeableConcept();
        codeableConcept.setText(specialNote.getText());

        fhirFlag.setCode(codeableConcept);

        resources.add(fhirFlag);

    }
}
