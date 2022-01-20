package org.apache.flink.kubernetes.operator.Utils;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;

import java.util.Collections;
import java.util.List;

public class KubernetesUtils {
	public static void setOwnerReference(HasMetadata owner, List<HasMetadata> resources) {
		final OwnerReference ownerReference = new OwnerReferenceBuilder()
			.withName(owner.getMetadata().getName())
			.withApiVersion(owner.getApiVersion())
			.withUid(owner.getMetadata().getUid())
			.withKind(owner.getKind())
			.withController(true)
			.withBlockOwnerDeletion(true)
			.build();
		resources.forEach(resource ->
			resource.getMetadata().setOwnerReferences(Collections.singletonList(ownerReference)));
	}
}
