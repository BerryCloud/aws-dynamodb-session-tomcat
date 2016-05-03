/*
 * Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.dynamodb.sessionmanager;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.catalina.Context;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.session.StoreBase;
import org.apache.catalina.util.CustomObjectInputStream;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import com.amazonaws.services.dynamodb.sessionmanager.util.DynamoUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

/**
 * Session store implementation that loads and stores HTTP sessions from Amazon
 * DynamoDB.
 */
public class DynamoDBSessionStore extends StoreBase {

	private static final Log logger = LogFactory.getLog(DynamoDBSessionStore.class);

	private static final String name = "AmazonDynamoDBSessionStore";
	// private static final String info = name + "/1.0";

	private AmazonDynamoDBClient dynamo;
	private String sessionTableName;

	private Set<String> keys = Collections.synchronizedSet(new HashSet<String>());
	private long keysTimestamp = 0;

	// @Override
	// public String getInfo() {
	// return info;
	// }

	@Override
	public String getStoreName() {
		return name;
	}

	public void setDynamoClient(AmazonDynamoDBClient dynamo) {
		this.dynamo = dynamo;
	}

	public void setSessionTableName(String tableName) {
		this.sessionTableName = tableName;
	}

	@Override
	public void clear() throws IOException {
		System.out.println("clear");
		final Set<String> keysCopy = new HashSet<String>();
		keysCopy.addAll(keys);

		new Thread("dynamodb-session-manager-clear") {
			@Override
			public void run() {
				for (String sessionId : keysCopy) {
					remove(sessionId);
				}
			}
		}.start();

	}

	@Override
	public int getSize() throws IOException {
		System.out.println("get size");
		TableDescription table = dynamo.describeTable(new DescribeTableRequest().withTableName(sessionTableName))
				.getTable();
		long itemCount = table.getItemCount();

		return (int) itemCount;
	}

	@Override
	public String[] keys() throws IOException {
		System.out.println("get keys");
		// refresh the keys stored in memory in every hour.
		if (keysTimestamp < System.currentTimeMillis() - 1000L * 60 * 60) {
			keysTimestamp = System.currentTimeMillis();
			System.out.println("keys size:" + keys.size());
			System.out.println("reload keys");
			// Other instances can also add or remove sessions, so we have to
			// synchronise the keys set with the DB sometimes
			List<String> list = DynamoUtils.loadKeys(dynamo, sessionTableName);
			System.out.println("loaded keys: " + list.size());
			keys.clear();
			keys.addAll(list);
		}
		System.out.println("keys size:" + keys.size());
		return keys.toArray(new String[keys.size()]);

	}

	@Override
	public Session load(String id) throws ClassNotFoundException, IOException {
		System.out.println("load " + id + " / " + sessionTableName);
		ByteBuffer byteBuffer = DynamoUtils.loadItemBySessionId(dynamo, sessionTableName, id);
		if (byteBuffer == null || byteBuffer.remaining() == 0) {
			keys.remove(id);
			return (null);
		}

		if (logger.isDebugEnabled()) {
			logger.debug("loading " + id + " / " + sessionTableName);
		}
		System.out.println("loading " + id + " / " + sessionTableName);

		ObjectInputStream ois = null;
		Loader loader = null;
		ClassLoader classLoader = null;
		ClassLoader oldThreadContextCL = Thread.currentThread().getContextClassLoader();

		try (ByteArrayInputStream fis = new ByteArrayInputStream(byteBuffer.array());
				BufferedInputStream bis = new BufferedInputStream(fis)) {
			Context context = getManager().getContext();
			if (context != null)
				loader = context.getLoader();
			if (loader != null)
				classLoader = loader.getClassLoader();
			if (classLoader != null) {
				Thread.currentThread().setContextClassLoader(classLoader);
				ois = new CustomObjectInputStream(bis, classLoader);
			} else {
				ois = new ObjectInputStream(bis);
			}
			StandardSession session = (StandardSession) manager.createEmptySession();
			session.readObjectData(ois);
			session.setManager(manager);
			keys.add(id);
			return (session);
		} finally {
			if (ois != null) {
				// Close the input stream
				try {
					ois.close();
				} catch (IOException f) {
					// Ignore
				}
			}
			Thread.currentThread().setContextClassLoader(oldThreadContextCL);
		}
	}

	@Override
	public void save(Session session) throws IOException {

		String id = session.getIdInternal();

		if (logger.isDebugEnabled()) {
			logger.debug("saving " + id + " / " + sessionTableName);
		}
		System.out.println("saving " + id + " / " + sessionTableName);

		ByteArrayOutputStream fos = new ByteArrayOutputStream();
		try (ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(fos))) {
			((StandardSession) session).writeObjectData(oos);
		}
		System.out.println("buffer size: " + fos.toByteArray().length);
		DynamoUtils.storeSession(dynamo, sessionTableName, id, ByteBuffer.wrap(fos.toByteArray()));
		System.out.println("saved");
		keys.add(id);
	}

	@Override
	public void remove(String id) {
		if (logger.isDebugEnabled()) {
			logger.debug("removing " + id + " / " + sessionTableName);
		}
		System.out.println("removing " + id + " / " + sessionTableName);
		DynamoUtils.deleteSession(dynamo, sessionTableName, id);
		keys.remove(id);
		System.out.println("removed	");
	}
}