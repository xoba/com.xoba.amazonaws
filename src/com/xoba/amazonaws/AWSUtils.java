package com.xoba.amazonaws;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;

public class AWSUtils {

	static {
		java.util.logging.Logger.getLogger("com.amazonaws.request").setLevel(Level.OFF);
	}

	public static interface IBucketListener {

		public boolean add(S3ObjectSummary s) throws Exception;

		public void done();

	}

	public static void scanObjectsInBucket(AmazonS3 s3, String bucket, IBucketListener listener) throws Exception {
		scanObjectsInBucket(s3, bucket, null, listener);
	}

	public static void scanObjectsInBucket(AmazonS3 s3, String bucket, String prefix, IBucketListener listener)
			throws Exception {
		ObjectListing list = (prefix == null ? s3.listObjects(bucket) : s3.listObjects(bucket, prefix));
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

	public static void deleteBucket(final AmazonS3 s3, final String bucket) throws Exception {

		final ExecutorService es = Executors.newFixedThreadPool(20);
		try {

			final List<Future<String>> results = new LinkedList<Future<String>>();

			scanObjectsInBucket(s3, bucket, new IBucketListener() {
				@Override
				public boolean add(final S3ObjectSummary s) {
					results.add(es.submit(new Callable<String>() {
						@Override
						public String call() throws Exception {
							s3.deleteObject(bucket, s.getKey());
							return s.getKey();
						}
					}));
					return true;
				}

				@Override
				public void done() {
				}
			});

			for (Future<String> s : results) {
				try {
					s.get();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			s3.deleteBucket(bucket);

		} finally {
			es.shutdown();
		}
	}

}
