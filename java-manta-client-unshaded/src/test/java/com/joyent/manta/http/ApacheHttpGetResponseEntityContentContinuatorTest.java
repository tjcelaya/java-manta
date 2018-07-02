/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.exception.ResumableDownloadException;
import com.joyent.manta.exception.ResumableDownloadIncompatibleRequestException;
import com.joyent.manta.exception.ResumableDownloadUnexpectedResponseException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static com.joyent.manta.http.ApacheHttpHeaderUtils.extractDownloadRequestFingerprint;
import static com.joyent.manta.http.ApacheHttpHeaderUtils.extractDownloadResponseFingerprint;
import static com.joyent.manta.http.ApacheHttpTestUtils.prepareResponseWithHeaders;
import static com.joyent.manta.http.ApacheHttpTestUtils.singleValueHeaderList;
import static com.joyent.manta.http.HttpRange.fromContentLength;
import static com.joyent.manta.util.MantaUtils.unmodifiableMap;
import static com.joyent.manta.util.UnitTestConstants.UNIT_TEST_URL;
import static org.apache.http.HttpHeaders.CONTENT_LENGTH;
import static org.apache.http.HttpHeaders.CONTENT_RANGE;
import static org.apache.http.HttpHeaders.ETAG;
import static org.apache.http.HttpHeaders.IF_MATCH;
import static org.apache.http.HttpHeaders.RANGE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

@Test
public class ApacheHttpGetResponseEntityContentContinuatorTest {

    private static final Logger LOG = LoggerFactory.getLogger(ApacheHttpGetResponseEntityContentContinuatorTest.class);

    public void httpGetIsCloneable() throws ResumableDownloadIncompatibleRequestException {

        final String initialRange = new HttpRange.Request(0, 2).render();

        final HttpGet get = new HttpGet(UNIT_TEST_URL);
        get.setHeaders(new Header[]{
                new BasicHeader(RANGE, initialRange),
                new BasicHeader(IF_MATCH, "abc")});

        final HttpGet cloned = ApacheHttpGetResponseEntityContentContinuator.cloneRequest(get);
        // we have to call getValue because most of the header classes dont actually equals themselves
        assertEquals(cloned.getFirstHeader(RANGE).getValue(), get.getFirstHeader(RANGE).getValue());

        cloned.setHeader(RANGE, new HttpRange.Request(1, 2).render());
        assertNotEquals(cloned.getFirstHeader(RANGE).getValue(), get.getFirstHeader(RANGE).getValue());
    }

    public void ctorRejectsInvalidInput() throws Exception {
        final MantaApacheHttpClientContext connCtx = mock(MantaApacheHttpClientContext.class);
        when(connCtx.getHttpClient()).thenReturn(mock(CloseableHttpClient.class));
        final HttpGet request = new HttpGet();
        final HttpResponse response =
                prepareResponseWithHeaders(
                        unmodifiableMap(
                                ETAG, singleValueHeaderList(ETAG, "a"),
                                CONTENT_LENGTH, singleValueHeaderList(CONTENT_LENGTH, "1")));
        final ResumableDownloadMarker marker = ResumableDownloadMarker.validateInitialExchange(
                extractDownloadRequestFingerprint(request),
                extractDownloadResponseFingerprint(response));

        // basic null checks
        assertThrows(NullPointerException.class,
                () -> new ApacheHttpGetResponseEntityContentContinuator(null, null, null, null));


        assertThrows(NullPointerException.class,
                () -> new ApacheHttpGetResponseEntityContentContinuator(connCtx, null, marker));

        assertThrows(NullPointerException.class,
                () -> new ApacheHttpGetResponseEntityContentContinuator(connCtx, request, null));

        // this connectionContext returns null for the getHttpClient call
        final MantaApacheHttpClientContext badConnCtx = mock(MantaApacheHttpClientContext.class);
        assertThrows(NullPointerException.class,
                () -> new ApacheHttpGetResponseEntityContentContinuator(badConnCtx, request, marker));

        // this connectionContext somehow doesn't support retry cancellation
        // e.g. a custom MantaConnectionFactory or MantaConnectionContext was supplied
        final MantaApacheHttpClientContext retryNotCancellableConnCtx = mock(MantaApacheHttpClientContext.class);
        when(retryNotCancellableConnCtx.getHttpClient()).thenReturn(mock(CloseableHttpClient.class));
        when(retryNotCancellableConnCtx.isRetryEnabled()).thenReturn(true);
        when(retryNotCancellableConnCtx.isRetryCancellable()).thenReturn(false);

        assertThrows(ResumableDownloadException.class,
                () -> new ApacheHttpGetResponseEntityContentContinuator(retryNotCancellableConnCtx, request, marker));
    }

    public void createMarkerValidatesHints() throws Exception {
        final String etag = new RandomStringGenerator.Builder().withinRange('a', 'z').build().generate(10);

        // any returned range is fine since none was specified
        final HttpRange.Response range = new HttpRange.Response(0, 1, 2L);

        // invalid ETag
        final String badEtag = etag.substring(1);

        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(etag, null),
                        ImmutablePair.of(null, null)));

        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(etag, null),
                        ImmutablePair.of(null, range)));

        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                ResumableDownloadMarker.validateInitialExchange(
                        ImmutablePair.of(etag, null), ImmutablePair.of(badEtag, range)));

        assertNotNull(
                ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(etag, null),
                        ImmutablePair.of(etag, range)));
    }

    /**
     * Tests validation of responses for downloads with an initial response code 200.
     */
    public void validateResponseExpectsNonNullHintsAndResponseFingerprint() throws Exception {
        final CloseableHttpClient client = mock(CloseableHttpClient.class);
        final HttpGet req = new HttpGet();

        final String etag = "abc";
        final long contentLength = 2;
        final HttpRange.Response contentRange = fromContentLength(HttpRange.Response.class, contentLength);
        final HttpResponse res = prepareResponseWithHeaders(
                unmodifiableMap(
                        ETAG, singleValueHeaderList(ETAG, etag),
                        CONTENT_LENGTH, singleValueHeaderList(CONTENT_LENGTH, Long.toString(contentLength)),
                        CONTENT_RANGE, singleValueHeaderList(CONTENT_RANGE, contentRange.render())));
        final ResumableDownloadMarker marker = ResumableDownloadMarker.validateInitialExchange(
                extractDownloadRequestFingerprint(req),
                extractDownloadResponseFingerprint(res));

        final ApacheHttpGetResponseEntityContentContinuator continuator =
                new ApacheHttpGetResponseEntityContentContinuator(client, req, marker, null);

        continuator.validateResponseWithMarker(ImmutablePair.of(etag, contentRange));

        // the following assertion just tests for programmer error
        assertThrows(NullPointerException.class, () ->
                continuator.validateResponseWithMarker(null));

        // the following assertions test a response with insufficient headers
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                continuator.validateResponseWithMarker(ImmutablePair.nullPair()));
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                continuator.validateResponseWithMarker(ImmutablePair.of(etag, null)));
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                continuator.validateResponseWithMarker(ImmutablePair.of(null, contentRange)));

        // the following assertions test a response with incorrect headers
        final String badEtag = etag.substring(1);
        final HttpRange.Response badContentRange = new HttpRange.Response(0, contentLength, contentLength + 1);

        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                continuator.validateResponseWithMarker(ImmutablePair.of(etag, badContentRange)));
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                continuator.validateResponseWithMarker(ImmutablePair.of(badEtag, contentRange)));
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                continuator.validateResponseWithMarker(ImmutablePair.of(badEtag, badContentRange)));
    }

}
