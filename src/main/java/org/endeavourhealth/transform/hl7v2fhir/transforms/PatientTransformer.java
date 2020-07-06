package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.*;
import ca.uhn.hl7v2.model.v23.segment.MRG;
import ca.uhn.hl7v2.model.v23.segment.PID;
import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.ResourceMergeMapHelper;
import org.endeavourhealth.transform.common.resourceBuilders.GenericBuilder;
import org.endeavourhealth.transform.fhirhl7v2.FhirHl7v2Filer;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class PatientTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    private static final ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    /**
     *
     * @param pid
     * @param patient
     * @return
     * @throws Exception
     */
    public static Patient transformPIDToPatient(PID pid, Patient patient) throws Exception {
        patient.getMeta().addProfile("http://endeavourhealth.org/fhir/StructureDefinition/primarycare-patient");

        CX[] patientIdList = pid.getPatientIDInternalID();
        ST id = patientIdList[0].getID();
        ST nhsNumber = patientIdList[1].getID();

        patient.addIdentifier().setValue(String.valueOf(id)).setSystem("http://imperial-uk.com/identifier/patient-id").setUse(Identifier.IdentifierUse.fromCode("secondary"));
        patient.addIdentifier().setSystem("http://fhir.nhs.net/Id/nhs-number")
                .setValue(String.valueOf(nhsNumber)).setUse(Identifier.IdentifierUse.fromCode("official"));

        XPN[] patientName = pid.getPatientName();
        ST familyName = patientName[0].getFamilyName();
        ST givenName = patientName[0].getGivenName();
        ID nameTypeCode = patientName[0].getNameTypeCode();
        ST prefix = patientName[0].getPrefixEgDR();
        patient.addName().addFamily(String.valueOf(familyName)).addPrefix(String.valueOf(prefix)).addGiven(String.valueOf(givenName)).setUse(HumanName.NameUse.OFFICIAL);

        IS gender = pid.getSex();
        switch(String.valueOf(gender)) {
            case "M":
                patient.setGender(Enumerations.AdministrativeGender.MALE);
                break;
            case "F":
                patient.setGender(Enumerations.AdministrativeGender.FEMALE);
                break;
            default:
                // code block
        }

        TS dob = pid.getDateOfBirth();
        if (!dob.isEmpty()) {
            String dtB = String.valueOf(dob.getTimeOfAnEvent());
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            Date date = formatter.parse(dtB.substring(0,4)+"-"+dtB.substring(4,6)+"-"+dtB.substring(6,8));
            patient.setBirthDate(date);
        }

        XAD[] patientAddress = pid.getPatientAddress();
        if(patientAddress != null && patientAddress.length > 0) {
            ID addType = patientAddress[0].getAddressType();
            ST city = patientAddress[0].getCity();
            ID country = patientAddress[0].getCountry();
            ST add = patientAddress[0].getStreetAddress();
            ST postCode = patientAddress[0].getZipOrPostalCode();

            Address address = new Address();
            if (String.valueOf(addType).equals("HOME")) {address.setUse(Address.AddressUse.HOME);}
            if (String.valueOf(addType).equals("TEMP")) {address.setUse(Address.AddressUse.TEMP);}
            if (String.valueOf(addType).equals("OLD")) {address.setUse(Address.AddressUse.OLD);}

            address.addLine(String.valueOf(add));
            address.setCountry(String.valueOf(country));
            address.setPostalCode(String.valueOf(postCode).replaceAll("\\s",""));
            address.setCity(String.valueOf(city));
            address.setDistrict("");
            patient.addAddress(address);
        }

        TS dod = pid.getPatientDeathDateAndTime();
        if (!dod.isEmpty()) {
            BooleanType bool = new BooleanType();
            bool.setValue(true);
            patient.setDeceased(bool);
        }
        return patient;
    }

    /**
     * A34 messages merge all content from one patient (minor patient) to another (the major patient)
     */
    public static void performA34PatientMerge(Bundle bundle, FhirHl7v2Filer.AdtResourceFiler filer, PID pid, MRG mrg) throws Exception {

        Parameters parameters = findParameters(bundle);

        CX[] patientIdList = pid.getPatientIDInternalID();
        String majorPatientId = String.valueOf(patientIdList[0].getID());
        String minorPatientId = String.valueOf(mrg.getPriorPatientIDInternal());

        LOG.debug("Doing A34 merge from minor patient " + minorPatientId + " to major patient " + majorPatientId);

        Map<String, String> idMappings = findIdMappings(parameters);

        //add the minor and major patient IDs to the ID map, so we change the patient references in our resources too
        String majorPatientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, majorPatientId);
        String minorPatientReference = ReferenceHelper.createResourceReference(ResourceType.Patient, minorPatientId);
        idMappings.put(minorPatientReference, majorPatientReference);

        List<ResourceWrapper> minorPatientResources = resourceRepository.getResourcesByPatient(filer.getServiceId(), UUID.fromString(minorPatientId));

        //copy the resources to the major patient
        for (ResourceWrapper minorPatientResource: minorPatientResources) {

            String json = minorPatientResource.getResourceData();
            Resource fhirOriginal = ParserPool.getInstance().parse(json);

            if (fhirOriginal instanceof Patient) {
                //we don't want to move patient resources, so just delete it
                filer.deletePatientResource(new GenericBuilder(fhirOriginal));

            } else {
                //for all other resources, re-map the IDs and save to the DB
                try {
                    //FHIR copy functions don't copy the ID or Meta, so deserialise twice instead
                    IdHelper.applyExternalReferenceMappings(fhirOriginal, idMappings, false);
                    filer.savePatientResource(new GenericBuilder(fhirOriginal));

                } catch (Exception ex) {
                    throw new Exception("Failed to save amended " + minorPatientResource.getResourceType() + " which originally had ID " + minorPatientResource.getResourceId() + " and now has " + fhirOriginal.getId(), ex);
                }

                LOG.debug("Moved " + fhirOriginal.getResourceType() + " " + fhirOriginal.getId());
            }
        }

        //need to wait here until all resources have been saved, otherwise if we let the below fn save the resource
        //mappings (of old patient to new patient) then the mapping gets applied when deleting the old patient (since
        //the delete is in a different thread). End result is that we end up KEEPING the minor patient and DELETING
        //the one we want to keep!
        filer.waitUntilEverythingIsSaved();

        //save these resource mappings for the future
        ResourceMergeMapHelper.saveResourceMergeMapping(filer.getServiceId(), idMappings);
    }

    private static Parameters findParameters(Bundle bundle) throws Exception {
        for (Bundle.BundleEntryComponent entry: bundle.getEntry()) {
            Resource resource = entry.getResource();
            if (resource.getResourceType() == ResourceType.Parameters) {
                return (Parameters)resource;
            }
        }

        throw new TransformException("Failed to find Parameters resource in Bundle");
    }

    private static String findParameterValue(Parameters parameters, String name) throws Exception {
        if (parameters.hasParameter()) {
            for (Parameters.ParametersParameterComponent component: parameters.getParameter()) {
                if (component.getName().equalsIgnoreCase(name)) {
                    StringType value = (StringType)component.getValue();
                    return value.getValue();
                }
            }
        }

        throw new TransformException("Failed to find parameter [" + name + "] in Parameters resource");
    }

    //returns a map of old Ids to new, formatted as FHIR references (e.g. Patient/<guid>)
    private static Map<String, String> findIdMappings(Parameters parameters) throws Exception {

        Map<String, String> referenceIdMap = new HashMap<>();

        if (parameters.hasParameter()) {
            for (Parameters.ParametersParameterComponent component: parameters.getParameter()) {
                if (component.getName().equalsIgnoreCase("OldToNewResourceMap")) {

                    for (Parameters.ParametersParameterComponent part: component.getPart()) {
                        String name = part.getName();
                        String value = ((StringType)part.getValue()).toString();

                        referenceIdMap.put(name, value);
                    }
                }
            }
        }

        LOG.debug("Id mappings are");
        for (String key: referenceIdMap.keySet()) {
            String value = referenceIdMap.get(key);
            LOG.debug(key + " -> " + value);
        }

        return referenceIdMap;
    }

}
