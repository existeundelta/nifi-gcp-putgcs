package com.synack.nifi.gcp.putgcs;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

/**
 *
 * @author Mikhail Sosonkin
 */
@Tags({"gcp", "gcs", "put"})
@CapabilityDescription("Consumer of GCP Pubsib topic")
@SeeAlso({})
@ReadsAttributes({})
@WritesAttributes({})
public class PutGcs extends AbstractProcessor {

    public static final PropertyDescriptor authProperty = new PropertyDescriptor.Builder().name("Authentication Keys")
            .description("Required if outside of GCP. OAuth token (contents of myproject.json)")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor bucketNameProperty = new PropertyDescriptor.Builder().name("Bucket name")
            .description("Destination Bucket")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor filenameProperty = new PropertyDescriptor.Builder().name("filename")
            .description("Destination file name")
            .defaultValue("${now():toNumber()}")
            .dynamic(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to upload.")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    // client state
    private Storage storage;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(authProperty);
        descriptors.add(bucketNameProperty);
        descriptors.add(filenameProperty);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if (storage == null) {
            StorageOptions.Builder opts = StorageOptions.getDefaultInstance().toBuilder();

            PropertyValue authKeys = context.getProperty(authProperty);
            if (authKeys.isSet()) {
                try {
                    opts = opts.setCredentials(ServiceAccountCredentials.fromStream(new ByteArrayInputStream(authKeys.getValue().getBytes())));
                } catch (Exception e) {
                    throw new ProcessException("Unable to set storage credentials", e);
                }
            }

            storage = opts.build().getService();

            if (storage == null) {
                throw new ProcessException("Unable to create storage");
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flow = session.get();

        String bucketName = context.getProperty(bucketNameProperty).getValue();
        String filename = context.getProperty(filenameProperty).evaluateAttributeExpressions(flow).getValue();

        getLogger().info("Uploading " + filename + " to " + bucketName);

        try {
            BlobInfo blobInfo;
            
            if(flow.getSize() < 1024 * 1024) {
                blobInfo
                        = storage.create(
                                BlobInfo.newBuilder(bucketName, filename).build(),
                                session.read(flow));
            } else {
                // It's recommended that larger uploads are done this way.
                blobInfo = BlobInfo.newBuilder(bucketName, filename).build();
            
                InputStream in = session.read(flow);

                try (WriteChannel writer = storage.writer(blobInfo)) {
                    byte[] buffer = new byte[8 * 1024];
                    int limit;

                    while ((limit = in.read(buffer)) >= 0) {
                        writer.write(ByteBuffer.wrap(buffer, 0, limit));
                    }
                }
            }

            session.remove(flow);
            
            getLogger().info("Done Uploading " + filename + ": " + blobInfo);
        } catch (Exception e) {
            session.transfer(flow, REL_FAILURE);
            
            getLogger().error("Error uploading: " + e.toString());
        }

        session.commit();
    }
}
