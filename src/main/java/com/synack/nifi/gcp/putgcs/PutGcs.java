package com.synack.nifi.gcp.putgcs;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
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

    public static final PropertyDescriptor bucketNameProperty = new PropertyDescriptor.Builder().name("Bucket name")
            .description("Destination Bucket")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor filenameProperty = new PropertyDescriptor.Builder().name("filename")
            .description("Destination base path")
            .defaultValue("${now()}")
            .dynamic(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles received from Pubsub.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private Storage storage;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(bucketNameProperty);
        descriptors.add(filenameProperty);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        //relationships.add(REL_SUCCESS);
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

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flow = session.get();

        if (flow == null) {
            return;
        }

        String bucketName = context.getProperty(bucketNameProperty).getValue();
        String filename = context.getProperty(filenameProperty).evaluateAttributeExpressions(flow).getValue();

        if (storage == null) {
            storage = StorageOptions.getDefaultInstance().getService();
        }

        BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, filename).build();
        
        getLogger().info("Writting " + filename + " to " + bucketName);

        try {
            InputStream in = session.read(flow);
            
            try (WriteChannel writer = storage.writer(blobInfo)) {
                byte[] buffer = new byte[1024];
                int limit;
                
                while ((limit = in.read(buffer)) >= 0) {
                    writer.write(ByteBuffer.wrap(buffer, 0, limit));
                }
            }
        } catch (Exception e) {
            throw new ProcessException("error uploading", e);
        }
        
        session.remove(flow);
        session.commit();
    }
}
