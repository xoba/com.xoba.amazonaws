package com.xoba.amazonaws;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;

public class AWSUtils {

	public static interface IBucketListener {

		public boolean add(S3ObjectSummary s);

		public void done();

	}

	public static void scanObjectsInBucket(AmazonS3 s3, String bucket, IBucketListener listener) throws Exception {
		scanObjectsInBucket(s3, bucket, null, listener);
	}

	public static void scanObjectsInBucket(AmazonS3 s3, String bucket, String prefix, IBucketListener listener)
			throws Exception {
		ObjectListing list = prefix == null ? s3.listObjects(bucket) : s3.listObjects(bucket, prefix);
		boolean done = false;
		while (!done) {
			List<S3ObjectSummary> ss = list.getObjectSummaries();
			Iterator<S3ObjectSummary> it = ss.iterator();
			while (it.hasNext() && !done) {
				S3ObjectSummary s = it.next();
				if (!listener.add(s)) {
					done = true;
				}
			}
			if (list.isTruncated()) {
				list = s3.listNextBatchOfObjects(list);
			} else {
				done = true;
			}
		}
		listener.done();
	}

	public static CreateQueueResult createSQS(AmazonSQS sqs, String name, int visibilityTimeoutSeconds) {
		return sqs.createQueue(new CreateQueueRequest(name).withAttributes(Collections.singletonMap(
				"VisibilityTimeout", new Integer(visibilityTimeoutSeconds).toString())));
	}

}
