package org.endeavourhealth.transform.ui.helpers;

import org.endeavourhealth.transform.ui.models.types.UIPeriod;
import org.hl7.fhir.instance.model.Period;

public class PeriodHelper {
	public static UIPeriod convert(Period period) {
		return new UIPeriod()
				.setStart(period.getStart())
				.setEnd(period.getEnd());
	}
}
