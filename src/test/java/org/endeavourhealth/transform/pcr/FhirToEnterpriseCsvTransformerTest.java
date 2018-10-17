package org.endeavourhealth.transform.pcr;

import com.fasterxml.jackson.databind.JsonNode;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformParams;
import org.endeavourhealth.transform.enterprise.FhirToEnterpriseCsvTransformer;
import org.endeavourhealth.transform.pcr.mocks.Mock_ResourceDAL;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.endeavourhealth.transform.enterprise.transforms.PatientTransformer;
import org.hl7.fhir.instance.model.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class FhirToEnterpriseCsvTransformerTest {
    private Mock_ResourceDAL resourceDAL;
    private List<ResourceWrapper> resources = new ArrayList<>();
    private JsonNode config;
    FhirToEnterpriseCsvTransformer transformer = new FhirToEnterpriseCsvTransformer();
    private EnterpriseTransformParams params;
    AbstractEnterpriseCsvWriter csvwriter;
    private static final Logger LOG = LoggerFactory.getLogger(FhirToEnterpriseCsvTransformer.class);
    //AbstractPcrCsvWriter personwriter = new AbstractPcrCsvWriter() {
    //}

    @Before
    public void setUp() throws Exception {
        resourceDAL = new Mock_ResourceDAL();

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void transformFromFhir() {
        List<Patient> patientList = new ArrayList<>();
        Patient p1 = new Patient();
        Address ad1 = new Address();
        ad1.addLine("1 address line");
        ad1.setCity("Gondor");
        ad1.setPostalCode("GO1 WT3");
        HumanName nom1 = new HumanName();
        nom1.addFamily("Smith");
        nom1.addGiven("John");
        Identifier id1 = new Identifier();
        id1.setUse(Identifier.IdentifierUse.OFFICIAL);
        id1.setId("id1");

        Class<PatientTransformer> clazz = PatientTransformer.class;
        try {
            Method method = clazz.getDeclaredMethod("transformResource", Long.class, Resource.class, AbstractEnterpriseCsvWriter.class, EnterpriseTransformParams.class);
            method.setAccessible(true); // This is the line
            method.invoke("transformResource",0L,p1, csvwriter,params);
        } catch(NoSuchMethodException nsfe) {
            nsfe.printStackTrace();
            fail();
        } catch(Exception nsfe) {
            nsfe.printStackTrace();

        }


//        try {
//
//            FhirToEnterpriseCsvTransformer.transformFromFhir(null,
//                    null, null,
//                    null, resources,
//                    "", null, "")
//        } catch (Exception e) {
//            //
//        }
    }

    @Test
    public void findCsvWriterForResourceType() {
    }

    @Test
    public void createTransformerForResourceType() {
    }
}