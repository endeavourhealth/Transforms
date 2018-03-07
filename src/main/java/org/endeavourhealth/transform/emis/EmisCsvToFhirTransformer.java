package org.endeavourhealth.transform.emis;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.ExchangeDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.xml.TransformErrorUtility;
import org.endeavourhealth.core.xml.transformError.TransformError;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.transforms.admin.*;
import org.endeavourhealth.transform.emis.csv.transforms.agreements.SharingOrganisationTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.appointment.SessionTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.appointment.SessionUserTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.appointment.SlotTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.careRecord.*;
import org.endeavourhealth.transform.emis.csv.transforms.coding.ClinicalCodeTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.coding.DrugCodeTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.prescribing.DrugRecordPreTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.prescribing.DrugRecordTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.prescribing.IssueRecordPreTransformer;
import org.endeavourhealth.transform.emis.csv.transforms.prescribing.IssueRecordTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class EmisCsvToFhirTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(EmisCsvToFhirTransformer.class);

    public static final String VERSION_5_4 = "5.4"; //version being received live from Emis as of Dec 2016
    public static final String VERSION_5_3 = "5.3"; //version being received live from Emis as of Nov 2016
    public static final String VERSION_5_1 = "5.1"; //version received in official emis test pack
    public static final String VERSION_5_0 = "5.0"; //assumed version received prior to emis test pack (not sure of actual version number)

    public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd"; //EMIS spec says "dd/MM/yyyy", but test data is different
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withHeader();   //EMIS csv files always contain a header

    public static void transform(UUID exchangeId, String exchangeBody, UUID serviceId, UUID systemId,
                                 TransformError transformError, List<UUID> batchIds, TransformError previousErrors) throws Exception {

        //for EMIS CSV, the exchange body will be a list of files received
        //split by /n but trim each one, in case there's a sneaky /r in there
        String[] files = ExchangeHelper.parseExchangeBodyIntoFileList(exchangeBody);

        LOG.info("Invoking EMIS CSV transformer for " + files.length + " files and service " + serviceId);

        String orgDirectory = FileHelper.validateFilesAreInSameDirectory(files);

        //we ignore the version already set in the exchange header, as Emis change versions without any notification,
        //so we dynamically work out the version when we load the first set of files
        String version = determineVersion(files);

        boolean processPatientData = shouldProcessPatientData(orgDirectory, files);

        //the processor is responsible for saving FHIR resources
        FhirResourceFiler processor = new FhirResourceFiler(exchangeId, serviceId, systemId, transformError, batchIds);

        Map<Class, AbstractCsvParser> parsers = new HashMap<>();

        try {
            //validate the files and, if this the first batch, open the parsers to validate the file contents (columns)
            createParsers(serviceId, systemId, exchangeId, files, version, parsers);

            LOG.trace("Transforming EMIS CSV content in " + orgDirectory);
            transformParsers(version, parsers, processor, previousErrors, processPatientData);

        } finally {
            closeParsers(parsers.values());
        }

        LOG.trace("Completed transform for service " + serviceId + " - waiting for resources to commit to DB");
        processor.waitToFinish();
    }

    /**
     * works out if we want to process (i.e. transform and store) the patient data from this extract,
     * which we don't if this extract is from before we received a later re-bulk from emis
     */
    private static boolean shouldProcessPatientData(String orgDirectory, String[] csvFiles) throws Exception {

        //find the extract date from one of the CSV file names
        Date extractDate = findExtractDate(csvFiles[0]);

        //our org GUID is the same as the directory name
        String orgGuid = new File(orgDirectory).getName();

        Date startDate = findStartDate(orgGuid);

        if (startDate == null
                || !extractDate.before(startDate)) {
            return true;

        } else {
            LOG.info("Not processing patient data for extract " + extractDate + " for org " + orgGuid + " as this is before their start date of " + startDate);
            return false;
        }
    }

    private static Date findStartDate(String orgGuid) throws Exception {

        Map<String, String> map = new HashMap<>();

        //this list of guids and dates is based off the live Emis extracts, giving the most recent bulk date for each organisation
        map.put("{DD31E915-7076-46CF-99CD-8378AB588B69}", "20/07/2017");
        map.put("{87A8851C-3DA4-4BE0-869C-3BF6BA7C0612}", "15/10/2017");
        map.put("{612DCB3A-5BE6-4D50-909B-F0F20565F9FC}", "09/08/2017");
        map.put("{15667F8D-46A0-4A87-9FA8-0C56B157A0A9}", "05/05/2017");
        map.put("{3CFEFBF9-B856-4A40-A39A-4EB6FA39295E}", "31/01/2017");
        map.put("{3F481450-AD19-4793-B1F0-40D5C2C57EF7}", "04/11/2017");
        map.put("{83939542-20E4-47C5-9883-BF416294BB22}", "13/10/2017");
        map.put("{73AA7E3A-4331-4167-8711-FE07DDBF4657}", "15/10/2017");
        map.put("{3B703CCF-C527-4EC8-A802-00D3B1535DD0}", "01/02/2017");
        map.put("{ED442CA3-351F-43E4-88A2-2EEACE39A402}", "13/10/2017");
        map.put("{86537B5B-7CF3-4964-8906-7C10929FBC20}", "13/05/2017");
        map.put("{9A4518C4-82CE-4509-8039-1B5F49F9C1FA}", "12/08/2017");
        map.put("{16D7F8F9-4A35-44B1-8F1D-DD0162584684}", "11/07/2017");
        map.put("{D392C499-345C-499B-898C-93F2CB8CC1B9}", "15/10/2017");
        map.put("{5B87882A-0EE8-4233-93D0-D2F5F4F94040}", "15/03/2017");
        map.put("{CFE3B460-9058-47FB-BF1D-6BEC13A2257D}", "19/04/2017");
        map.put("{7B03E105-9275-47CC-8022-1469FE2D6AE4}", "20/04/2017");
        map.put("{94470227-587C-47D7-A51F-9893512424D8}", "27/04/2017");
        map.put("{734F4C99-6326-4CA4-A22C-632F0AC12FFC}", "17/10/2017");
        map.put("{03C5B4B4-1A70-45F8-922E-135C826D48E0}", "20/04/2017");
        map.put("{1BB17C3F-CE80-4261-AF6C-BE987E3A5772}", "09/05/2017");
        map.put("{16F6DD42-2140-4395-95D5-3FA50E252896}", "20/04/2017");
        map.put("{3B6FD632-3FFB-48E6-9775-287F6C486752}", "15/10/2017");
        map.put("{F987F7BD-E19C-46D2-A446-913489F1BB7A}", "05/02/2017");
        map.put("{BE7CC1DC-3CAB-4BB1-A5A2-B0C854C3B78E}", "06/07/2017");
        map.put("{303EFA4E-EC8F-4CBC-B629-960E4D799E0D}", "15/10/2017");
        map.put("{5EE8FD1F-F23A-4209-A1EE-556F9350C900}", "01/02/2017");
        map.put("{04F6C555-A298-45F1-AC5E-AC8EBD2BB720}", "17/10/2017");
        map.put("{67383254-F7F1-4847-9AA9-C7DCF32859B8}", "17/10/2017");
        map.put("{31272E4E-40E0-4103-ABDC-F40A7B75F278}", "19/10/2017");
        map.put("{09CA2E3B-7143-4999-9934-971F3F2E6D8C}", "15/10/2017");
        map.put("{0527BCE2-4315-47F2-86A1-2E9F3E50399B}", "15/10/2017");
        map.put("{16DD14B5-D1D5-4B0C-B886-59AC4DACDA7A}", "04/07/2017");
        map.put("{411D0A79-6913-473C-B486-C01F6430D8A6}", "21/09/2017");
        map.put("{0862FADA-594A-415E-B971-7A4312E0A58C}", "10/06/2017");
        map.put("{249C3F3C-24F0-44CE-97A9-B535982BD70C}", "15/10/2017");
        map.put("{5D7A1915-6E22-4B20-A8AE-4768C06D3BBF}", "28/09/2017"); //Barts community
        map.put("{131AE556-8B50-4C17-9D7D-A4B19F7B1FEA}", "15/10/2017");
        map.put("{C0D2D0DF-EF78-444D-9A6D-B9EDEF5EF350}", "13/10/2017");
        map.put("{F174B354-4156-4BCB-960F-35D0145075EA}", "01/02/2017");
        map.put("{38600D63-1DE0-4910-8ED6-A38DC28A9DAA}", "19/02/2018"); //THE SPITALFIELDS PRACTICE (CDB 16);F84081
        map.put("{B3ECA2DE-D926-4594-B0EA-CF2F28057CE1}", "19/10/2017");
        map.put("{18F7C28B-2A54-4F82-924B-38C60631FFFA}", "04/02/2018"); //Rowans Surgery (CDB 18174);H85035
        map.put("{16FB5EE8-5039-4068-BC42-1DB56DC2A530}", "08/06/2017");
        map.put("{4BA4A5AC-7B25-40B2-B0EA-135702A72F9D}", "15/10/2017");
        map.put("{01B8341F-BC8F-450E-8AFA-4CDA344A5009}", "15/10/2017");
        map.put("{E6FBEA1C-BDA2-40B7-A461-C262103F08D7}", "08/06/2017");
        map.put("{141C68EB-1BC8-4E99-A9D9-0E63A8944CA9}", "15/10/2017");
        map.put("{A3EA804D-E7EB-43EE-8F1F-E860F6337FF7}", "15/10/2017");
        map.put("{771B42CC-9C0C-46E2-8143-76F04AF91AD5}", "13/11/2017"); //cranwich road
        map.put("{16EA8D5C-C667-4818-B629-5D6F4300FEEF}", "11/05/2017");
        map.put("{29E51964-C94D-4CB4-894E-EB18E27DEFC1}", "15/10/2017");
        map.put("{3646CCA5-7FE4-4DFE-87CD-DA3CE1BA885D}", "27/09/2017");
        map.put("{3EC82820-702F-4218-853B-D3E5053646A8}", "05/05/2017");
        map.put("{37F3E676-B203-4329-97F8-2AF5BFEAEE5A}", "19/10/2017");
        map.put("{A0E3208B-95E9-4284-9B5A-D4D387CCC9F9}", "07/06/2017");
        map.put("{0BEAF1F0-9507-4AC2-8997-EC0BA1D0247E}", "19/10/2017");
        map.put("{071A50E7-1764-4210-94EF-6A4BF96CF753}", "21/02/2017");
        map.put("{0C1983D8-FB7D-4563-84D0-1F8F6933E786}", "20/07/2017");
        map.put("{871FEEB2-CE30-4603-B9A3-6FA6CC47B5D4}", "15/10/2017");
        map.put("{42906EBE-8628-486D-A52F-27B935C9937A}", "01/02/2017");
        map.put("{1AB7ABF3-2572-4D07-B719-CFB2FE3AAC80}", "15/10/2017");
        map.put("{E312A5B7-13E7-4E43-BE35-ED29F6216D3C}", "20/04/2017");
        map.put("{55E60891-8827-40CD-8011-B0223D5C8970}", "15/10/2017");
        map.put("{03A63F52-7FEE-4592-9B54-83CEBCF67B5D}", "26/04/2017");
        map.put("{DB39B649-B48D-4AC2-BAB1-AC807AABFAC4}", "15/10/2017");
        map.put("{0AF9B2AF-A0FB-40B0-BA05-743BA6845DB1}", "26/08/2017");
        map.put("{A7600092-319C-4213-92C2-738BEEFC1609}", "31/01/2017");
        map.put("{5A1AABA9-7E96-41E7-AF18-E02F4CF1DFB6}", "15/10/2017");
        map.put("{7D8CE31D-66AA-4D6A-9EFD-313646BD1D73}", "15/10/2017");
        map.put("{03EA4A79-B6F1-4524-9D15-992B47BCEC9A}", "15/10/2017");
        map.put("{4588C493-2EA3-429A-8428-E610AE6A6D76}", "28/09/2017"); //Barts community
        map.put("{B13F3CC9-C317-4E0D-9C57-C545E4A53CAF}", "15/10/2017");
        map.put("{463DA820-6EC4-48CB-B915-81B31AFBD121}", "13/10/2017");
        map.put("{16F0D65C-B2A8-4186-B4E7-BBAF4390EC55}", "13/10/2017");
        map.put("{0039EF15-2DCF-4F70-B371-014C807210FD}", "24/05/2017");
        map.put("{E132BF05-78D9-4E4B-B875-53237E76A0FA}", "19/10/2017");
        map.put("{3DFC2DA6-AD8C-4836-945D-A6F8DB22AA49}", "15/10/2017");
        map.put("{BCB43B1D-2857-4186-918B-460620F98F81}", "13/10/2017");
        map.put("{E134C74E-FA3E-4E14-A4BB-314EA3D3AC16}", "15/10/2017");
        map.put("{C0F40044-C2CA-4D1D-95D3-553B29992385}", "26/08/2017");
        map.put("{B174A018-538D-4065-838C-023A245B53DA}", "14/02/2017");
        map.put("{43380A69-AE7D-4ED7-B014-0708675D0C02}", "08/06/2017");
        map.put("{E503F0E0-FE56-4CEF-BAB5-0D25B834D9BD}", "13/10/2017");
        map.put("{08946F29-1A53-4AF2-814B-0B8758112F21}", "07/02/2018"); //NEWHAM MEDICAL CENTRE (CDB 3461);F84669
        map.put("{09857684-535C-4ED6-8007-F91F366611C6}", "19/10/2017");
        map.put("{C409A597-009A-4E11-B828-A595755DE0EA}", "17/10/2017");
        map.put("{58945A1C-2628-4595-8F8C-F75D93045949}", "15/10/2017");
        map.put("{16FF2874-20B0-4188-B1AF-69C97055AA60}", "17/10/2017");
        map.put("{2C91E9DA-3F92-464E-B6E6-61D3DE52E62F}", "15/10/2017");
        map.put("{16E7AD27-2AD9-43C0-A473-1F39DF93E981}", "10/06/2017");
        map.put("{A528478D-65DB-435C-9E98-F8BDB49C9279}", "20/04/2017");
        map.put("{A2BDB192-E79C-44C5-97A2-1FD4517C456F}", "21/08/2017");
        map.put("{73DFF193-E917-4DBC-B5CF-DD2797B29377}", "15/10/2017");
        map.put("{62825316-9107-4E2C-A22C-86211B4760DA}", "13/10/2017");
        map.put("{006E8A30-2A45-4DBE-91D7-1C53FADF38B1}", "28/01/2018"); //The Lawson Practice (CDB 4334);F84096
        map.put("{E32AA6A6-46B1-4198-AA13-058038AB8746}", "13/10/2017");
        map.put("{B51160F1-79E3-4BA7-AA3D-1112AB341146}", "30/09/2017");
        map.put("{234503E5-56B4-45A0-99DA-39854FBE78E9}", "01/02/2017");
        map.put("{7D1852DA-E264-4599-B9B4-8F40207F967D}", "09/10/2017");
        map.put("{44716213-7FEE-4247-A09E-7285BD6B69C6}", "13/10/2017");
        map.put("{19BCC870-2704-4D21-BA7B-56F2F472AF35}", "15/10/2017");
        map.put("{FEF842DA-FD7C-480F-945A-D097910A81EB}", "13/10/2017");
        map.put("{1C980E19-4A39-4ACD-BA8A-925D3E525765}", "13/10/2017");
        map.put("{AABDDC3A-93A4-4A87-9506-AAF52E74012B}", "07/02/2018"); //DR N DRIVER AND PARTNERS (CDB 4419);F84086
        map.put("{90C2959C-0C2D-43DC-A81B-4AD594C17999}", "20/04/2017");
        map.put("{1F1669CF-1BB0-47A7-8FBF-BE65651644C1}", "15/10/2017");
        map.put("{C1800BE8-4C1D-4340-B0F2-7ED208586ED3}", "15/10/2017");
        map.put("{55A94703-4582-46FB-808A-1990E9CBCB6F}", "19/02/2018"); //Stamford Hill Group Practice (CDB 56);F84013
        map.put("{D4996E62-268F-4759-83A6-7A68D0B38CEC}", "27/04/2017");
        map.put("{3C843BBA-C507-4A95-9934-1A85B977C7B8}", "01/02/2017");
        map.put("{2216253B-705D-4C46-ADB3-ED48493D6A39}", "03/02/2018"); //RIVERSIDE MEDICAL PRACTICE (CDB 14675);Y01962
        map.put("{00123F97-4557-44AD-81B5-D9902DD72EE9}", "28/04/2017");
        map.put("{E35D4D12-E7D2-484B-BFF6-4653B3FED228}", "15/10/2017");
        map.put("{6D8B4D28-838B-4915-A148-6FEC2CEBCE77}", "05/07/2017");
        map.put("{188D5B4D-4BF6-46E3-AF11-3AD32C68D251}", "19/10/2017");
        map.put("{16F7DDE1-3763-4D3A-A58D-F12F967718CF}", "02/11/2017");
        map.put("{03148933-6E1C-4A8A-A6D2-A3D488E14DDD}", "30/12/2017");
        map.put("{16DE1A3C-875B-4AB2-B227-8A42604E029C}", "05/11/2017");
        map.put("{D628D1BC-D02E-4101-B8CD-5B3DB2D06FC1}", "05/05/2017");
        map.put("{1EA6259A-6A49-46DB-991D-D604675F87E2}", "15/10/2017");
        map.put("{817F9B46-AEE0-45D5-95E3-989F75C4844E}", "20/04/2017");
        map.put("{1C422471-F52A-4C30-8D23-140BEB7AAEFC}", "15/08/2017");
        map.put("{A6467E73-0F15-49D6-AFAB-4DFB487E7963}", "10/05/2017");
        map.put("{CC7D1781-1B85-4AD6-A5DD-9AD5E092E8DB}", "13/10/2017");
        map.put("{167CD5C8-148F-4D78-8997-3B22EC0AF6B6}", "13/10/2017");
        map.put("{9DD5D2CE-2585-49D8-AF04-2CB1BD137594}", "15/10/2017");
        map.put("{D6696BB5-DE69-49D1-BC5E-C56799E42640}", "07/02/2018"); //BOLEYN MEDICAL CENTRE (CDB 4841);F84050
        map.put("{169375A9-C3AB-4C5E-82B0-DFF7656AD1FA}", "20/04/2017");
        map.put("{0A8ECFDE-95EE-4811-BC05-668D49F5C799}", "19/11/2017");
        map.put("{79C898A1-BB92-48F9-B0C3-6725370132B5}", "20/10/2017");
        map.put("{472AC9BA-AFFE-4E81-81CA-40DD8389784D}", "27/04/2017");
        map.put("{00121CB7-76A6-4D57-8260-E9CA62FFCD77}", "13/10/2017");
        map.put("{0FCBA0A7-7CAB-4E75-AC81-5041CD869CA1}", "15/10/2017");
        map.put("{00A9C32D-2BB2-4A20-842A-381B3F2031C0}", "19/10/2017");
        map.put("{26597C5A-3E29-4960-BE11-AC75D0430615}", "03/05/2017");
        map.put("{D945FEF7-F5EF-422B-AB35-6937F9792B54}", "15/10/2017");
        map.put("{16D685C6-130A-4B19-BCA9-90AC7DC16346}", "08/07/2017");
        map.put("{F09E9CEF-2615-4C9D-AA3D-79E0AB10D0B3}", "13/10/2017");
        map.put("{CD7EF748-DB88-49CF-AA6E-24F65029391F}", "15/10/2017");
        map.put("{B22018CF-2B52-4A1A-9F6A-CEA13276DB2E}", "19/10/2017");
        map.put("{0DF8CFC7-5DE6-4DDB-846A-7F28A2740A00}", "02/12/2017");
        map.put("{50F439E5-DB18-43A0-9F25-825957013A07}", "11/01/2018"); //DR PI ABIOLA (CDB 5681);F84631
        map.put("{00A3BA25-21C6-42DE-82AA-55FF0D85A6C3}", "02/01/2018"); //NOT FIXED YET - MARKET STREET HEALTH GROUP (CDB 381);F84004
        map.put("{77B59D29-0FD9-4737-964F-5DBA49D94AB6}", "02/01/2018"); //NOT FIXED YET - Star Lane Medical Centre (CDB 40);F84017
        map.put("{91239362-A105-4DEA-8E8E-239C3BCEDFD2}", "11/01/2018"); //BEECHWOOD MEDICAL CENTRE (CDB 5661);F84038

        String startDateStr = map.get(orgGuid);
        if (Strings.isNullOrEmpty(startDateStr)) {
            return null;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        return sdf.parse(startDateStr);
    }


    private static void closeParsers(Collection<AbstractCsvParser> parsers) {
        for (AbstractCsvParser parser : parsers) {
            try {
                parser.close();
            } catch (IOException ex) {
                //don't worry if this fails, as we're done anyway
            }
        }
    }

    /**
     * the Emis schema changes without notice, so rather than define the version in the SFTP reader,
     * we simply look at the files to work out what version it really is
     */
    public static String determineVersion(String[] files) throws Exception {

        List<String> possibleVersions = new ArrayList<>();
        possibleVersions.add(VERSION_5_4);
        possibleVersions.add(VERSION_5_3);
        possibleVersions.add(VERSION_5_1);
        possibleVersions.add(VERSION_5_0);

        for (String filePath: files) {

            //create a parser for the file but with a null version, which will be fine since we never actually parse any data from it
            AbstractCsvParser parser = createParserForFile(null, null, null, null, filePath);

            //calling this will return the possible versions that apply to this parser
            possibleVersions = parser.testForValidVersions(possibleVersions);
        }

        //if we end up with one or more possible versions that do apply, then
        //return the first, since that'll be the most recent one
        if (!possibleVersions.isEmpty()) {
            return possibleVersions.get(0);
        }

        throw new TransformException("Unable to determine version for EMIS CSV");
    }
    /*public static String determineVersion(File dir) throws Exception {

        String[] versions = new String[]{VERSION_5_0, VERSION_5_1, VERSION_5_3, VERSION_5_4};
        Exception lastException = null;

        for (String version: versions) {

            Map<Class, AbstractCsvParser> parsers = new HashMap<>();
            try {
                validateAndOpenParsers(dir, version, true, parsers);

                //if we make it here, this version is the right one
                return version;

            } catch (Exception ex) {
                //ignore any exceptions, as they just mean the version is wrong, so try the next one
                lastException = ex;

            } finally {
                //make sure to close any parsers that we opened
                closeParsers(parsers.values());
            }
        }

        throw new TransformException("Unable to determine version for EMIS CSV", lastException);
    }*/


    /*private static File validateAndFindCommonDirectory(String sharedStoragePath, String[] files) throws Exception {
        String organisationDir = null;

        for (String file: files) {
            File f = new File(sharedStoragePath, file);
            if (!f.exists()) {
                LOG.error("Failed to find file {} in shared storage {}", file, sharedStoragePath);
                throw new FileNotFoundException("" + f + " doesn't exist");
            }
            //LOG.info("Successfully found file {} in shared storage {}", file, sharedStoragePath);

            try {
                File orgDir = f.getParentFile();

                if (organisationDir == null) {
                    organisationDir = orgDir.getAbsolutePath();
                } else {
                    if (!organisationDir.equalsIgnoreCase(orgDir.getAbsolutePath())) {
                        throw new Exception();
                    }
                }

            } catch (Exception ex) {
                throw new FileNotFoundException("" + f + " isn't in the expected directory structure within " + organisationDir);
            }

        }
        return new File(organisationDir);
    }*/


    private static void createParsers(UUID serviceId, UUID systemId, UUID exchangeId, String[] files, String version, Map<Class, AbstractCsvParser> parsers) throws Exception {

        for (String filePath: files) {

            AbstractCsvParser parser = createParserForFile(serviceId, systemId, exchangeId, version, filePath);
            Class cls = parser.getClass();
            parsers.put(cls, parser);
        }
    }

    private static AbstractCsvParser createParserForFile(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        String fName = FilenameUtils.getName(filePath);
        String[] toks = fName.split("_");

        String domain = toks[1];
        String name = toks[2];

        //need to camel case the domain
        String first = domain.substring(0, 1);
        String last = domain.substring(1);
        domain = first.toLowerCase() + last;

        String clsName = "org.endeavourhealth.transform.emis.csv.schema." + domain + "." + name;
        Class cls = Class.forName(clsName);

        //now construct an instance of the parser for the file we've found
        Constructor<AbstractCsvParser> constructor = cls.getConstructor(UUID.class, UUID.class, UUID.class, String.class, String.class);
        return constructor.newInstance(serviceId, systemId, exchangeId, version, filePath);
    }


    private static String findDataSharingAgreementGuid(Map<Class, AbstractCsvParser> parsers) throws Exception {

        //we need a file name to work out the data sharing agreement ID, so just the first file we can find
        String firstFilePath = parsers
                .values()
                .iterator()
                .next()
                .getFilePath();

        String name = FilenameUtils.getBaseName(firstFilePath); //file name without extension
        String[] toks = name.split("_");
        if (toks.length != 5) {
            throw new TransformException("Failed to extract data sharing agreement GUID from filename " + firstFilePath);
        }
        return toks[4];
    }

    private static Date findExtractDate(String filePath) throws Exception {
        String name = FilenameUtils.getBaseName(filePath);
        String[] toks = name.split("_");
        if (toks.length != 5) {
            throw new TransformException("Failed to find extract date in filename " + filePath);
        }
        String dateStr = toks[3];
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.parse(dateStr);
    }

    private static void transformParsers(String version,
                                         Map<Class, AbstractCsvParser> parsers,
                                         FhirResourceFiler fhirResourceFiler,
                                         TransformError previousErrors,
                                         boolean processPatientData) throws Exception {

        boolean allowProcessingDisabledServices = TransformConfig.instance().isEmisAllowDisabledOrganisations();
        boolean allowMissingCodes = TransformConfig.instance().isEmisAllowMissingCodes();
        String sharingAgreementGuid = findDataSharingAgreementGuid(parsers);

        if (!processPatientData) {
            //if we've already decided that we're not going to process the patient data,
            //then we've already handled the fact that this service will be disabled,
            //so allow the extract to be processed
            allowProcessingDisabledServices = true;
        }

        EmisCsvHelper csvHelper = new EmisCsvHelper(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId(),
                                                    fhirResourceFiler.getExchangeId(), sharingAgreementGuid,
                                                    allowProcessingDisabledServices, allowMissingCodes);

        //if this is the first extract for this organisation, we need to apply all the content of the admin resource cache
        ExchangeDalI exchangeDal = DalProvider.factoryExchangeDal();
        if (!exchangeDal.isServiceStarted(fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId())) {
            LOG.trace("Applying admin resource cache for service {} and system {}", fhirResourceFiler.getServiceId(), fhirResourceFiler.getSystemId());

            csvHelper.applyAdminResourceCache(fhirResourceFiler);
            AuditWriter.writeExchangeEvent(fhirResourceFiler.getExchangeId(), "Applied Emis Admin Resource Cache");
        }

        LOG.trace("Starting pre-transforms to cache data");

        //check the sharing agreement to see if it's been disabled
        SharingOrganisationTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

        //these transforms don't create resources themselves, but cache data that the subsequent ones rely on
        ClinicalCodeTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        DrugCodeTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        OrganisationLocationTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        SessionUserTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

        if (processPatientData) {
            PatientPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            ProblemPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            ObservationPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            DrugRecordPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            IssueRecordPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            DiaryPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            ConsultationPreTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        }

        //before getting onto the files that actually create FHIR resources, we need to
        //work out what record numbers to process, if we're re-running a transform
        boolean processingSpecificRecords = findRecordsToProcess(parsers, previousErrors);

        LOG.trace("Starting admin transforms");

        //run the transforms for non-patient resources
        LocationTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        OrganisationTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        UserInRoleTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
        SessionTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

        //if this extract is one of the ones from BEFORE we got a subsequent re-bulk, we don't want to process
        //the patient data in the extract, as we know we'll be getting a later extract saying to delete it and then
        //another extract to replace it
        if (processPatientData) {

            LOG.trace("Starting patient transforms");

            //note the order of these transforms is important, as consultations should be before obs etc.
            PatientTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            ConsultationTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            IssueRecordTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            DrugRecordTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            SlotTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            DiaryTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            ObservationReferralTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            ProblemTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);
            ObservationTransformer.transform(version, parsers, fhirResourceFiler, csvHelper);

            if (!processingSpecificRecords) {

                //if we have any new Obs, Conditions, Medication etc. that reference pre-existing parent obs or problems,
                //then we need to retrieve the existing resources and update them
                csvHelper.processRemainingObservationParentChildLinks(fhirResourceFiler);

                //process any new items linked to past consultations
                csvHelper.processRemainingNewConsultationRelationships(fhirResourceFiler);

                //if we have any changes to the staff in pre-existing sessions, we need to update the existing FHIR Schedules
                //Confirmed on Live data - we NEVER get an update to a session_user WITHOUT also an update to the session
                //csvHelper.processRemainingSessionPractitioners(fhirResourceFiler);

                //process any changes to ethnicity or marital status, without a change to the Patient
                csvHelper.processRemainingEthnicitiesAndMartialStatuses(fhirResourceFiler);

                //process any changes to Org-Location links without a change to the Location itself
                csvHelper.processRemainingOrganisationLocationMappings(fhirResourceFiler);

                //process any changes to Problems that didn't have an associated Observation change too
                csvHelper.processRemainingProblems(fhirResourceFiler);

                //if we have any new Obs etc. that refer to pre-existing problems, we need to update the existing FHIR Problem
                csvHelper.processRemainingProblemRelationships(fhirResourceFiler);

                //update any MedicationStatements to set the last issue date on them
                csvHelper.processRemainingMedicationIssueDates(fhirResourceFiler);
            }
        }
    }


    public static boolean findRecordsToProcess(Map<Class, AbstractCsvParser> allParsers, TransformError previousErrors) throws Exception {

        boolean processingSpecificRecords = false;

        for (AbstractCsvParser parser: allParsers.values()) {

            String filePath = parser.getFilePath();
            String fileName = FilenameUtils.getName(filePath);

            Set<Long> recordNumbers = TransformErrorUtility.findRecordNumbersToProcess(fileName, previousErrors);
            parser.setRecordNumbersToProcess(recordNumbers);

            //if we have a non-null set, then we're processing specific records in some file
            if (recordNumbers != null) {
                processingSpecificRecords = true;
            }
        }

        return processingSpecificRecords;
    }


}
