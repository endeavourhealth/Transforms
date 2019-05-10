package org.endeavourhealth.transform.ui.helpers;

import org.endeavourhealth.common.fhir.AddressHelper;
import org.endeavourhealth.transform.ui.models.types.UIAddress;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.StringType;

import java.util.List;

public class UIAddressHelper {


	public static UIAddress getHomeAddress(Patient patient) {

		//got inconsistencies between this class and everywhere else, so changing
		//to use common function for getting "best" name for a patient, which also factors
		//in that names may have end dates
		Address addr = AddressHelper.findHomeAddress(patient);
		if (addr != null) {
			return transform(addr);
		} else {
			return null;
		}

		/*List<Address> addresses = patient.getAddress();
		if (addresses == null)
			return null;

		Address homeAddress = addresses
				.stream()
				.filter(t -> (t.getUse() != null) && (t.getUse() == Address.AddressUse.HOME))
				.filter(t -> (t.getPeriod() == null) || (t.getPeriod().getEnd() == null))
				.collect(StreamExtension.firstOrNullCollector());

		if (homeAddress == null) {
			homeAddress =
					addresses
							.stream()
							.filter(t -> t.getUse() == null)
							.filter(t -> (t.getPeriod() == null) || t.getPeriod().getEnd() == null)
							.collect(StreamExtension.firstOrNullCollector());
		}

		if (homeAddress == null)
			return null;

		return transform(homeAddress);*/
	}

	public static UIAddress transform(Address homeAddress) {
		return new UIAddress()
				.setLine1(getLine(homeAddress.getLine(), 0))
				.setLine2(getLine(homeAddress.getLine(), 1))
				.setLine3(getLine(homeAddress.getLine(), 2))
				.setDistrict(homeAddress.getDistrict())
				.setCity(homeAddress.getCity())
				.setPostalCode(homeAddress.getPostalCode())
				.setCountry(homeAddress.getCountry())
				.setUse(getUse(homeAddress.getUse()));
	}

	public static String getLine(List<StringType> lines, int lineNumber) {
		if (lines == null)
			return "";

		if (lineNumber >= lines.size())
			return "";

		return lines.get(lineNumber).getValueNotNull();
	}

	private static String getUse(Address.AddressUse addressUse) {
		if (addressUse != null)
			return addressUse.getDisplay();

		return null;
	}
}
