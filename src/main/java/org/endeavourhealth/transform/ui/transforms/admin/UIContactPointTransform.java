package org.endeavourhealth.transform.ui.transforms.admin;

import org.endeavourhealth.transform.ui.models.resources.admin.UIContactPoint;
import org.hl7.fhir.instance.model.ContactPoint;

import java.util.ArrayList;
import java.util.List;

public class UIContactPointTransform {
	public static List<UIContactPoint> transform(List<ContactPoint> contactPoints) {
		List<UIContactPoint> uiContactPoints = new ArrayList<>();

		for(ContactPoint contactPoint : contactPoints) {
			UIContactPoint uiContactPoint = new UIContactPoint()
					.setSystem(getSystem(contactPoint.getSystem()))
					.setValue(contactPoint.getValue())
					.setUse(getUse(contactPoint.getUse()));

			uiContactPoints.add(uiContactPoint);
		}

		return uiContactPoints;
	}

	private static String getUse(ContactPoint.ContactPointUse contactPointUse) {
		if (contactPointUse != null)
			return contactPointUse.getDisplay();

		return null;
	}

	private static String getSystem(ContactPoint.ContactPointSystem contactPointSystem) {
		if (contactPointSystem != null)
			return contactPointSystem.getDisplay();

		return null;
	}
}
