/**
 * Copyright 2016-2019 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.reaktivity.nukleus.mqtt.internal;

public final class MqttReasonCodes
{
    public static final int SUCCESS = 0x00;

    public static final int NORMAL_DISCONNECT = 0x00;

    public static final int GRANTED_QOS_1 = 0x00;
    public static final int GRANTED_QOS_2 = 0x01;
    public static final int GRANTED_QOS_3 = 0x02;

    public static final int DISCONNECT_WITH_WILL_MESSAGE = 0x04;

    public static final int NO_MATCHING_SUBSCRIBERS = 0x10;
    public static final int NO_SUBSCRIPTION_EXISTED = 0x11;

    public static final int CONTINUE_AUTHENTICATION = 0x18;
    public static final int REAUTHENTICATE = 0x19;

    public static final int UNSPECIFIED_ERROR = 0x80;
    public static final int MALFORMED_PACKET = 0x81;
    public static final int PROTOCOL_ERROR = 0x82;
    public static final int IMPLEMENTATION_SPECIFIC_ERROR = 0x83;
    public static final int UNSUPPORTED_PROTOCOL_VERSION = 0x84;
    public static final int CLIENT_IDENTIFIER_NOT_VALID = 0x85;
    public static final int BAD_USER_NAME_OR_PASSWORD = 0x86;
    public static final int NOT_AUTHORIZED = 0x87;
    public static final int SERVER_UNAVAILABLE = 0x88;
    public static final int SERVER_BUSY = 0x89;
    public static final int BANNED = 0x8A;
    public static final int SERVER_SHUTTING_DOWN = 0x8B;
    public static final int BAD_AUTHENTICATION_METHOD = 0x8C;

    public static final int KEEP_ALIVE_TIMEOUT = 0x8D;
    public static final int SESSION_TAKEN_OVER = 0x8E;

    public static final int TOPIC_FILTER_INVALID = 0x8F;
    public static final int TOPIC_NAME_INVALID = 0x90;

    public static final int PACKET_IDENTIFIER_IN_USE = 0x91;
    public static final int PACKET_IDENTIFIER_NOT_FOUND = 0x92;

    public static final int SHARED_SUBSCRIPTIONS_NOT_SUPPORTED = 0x9E;

    public static final int SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED = 0xA1;
    public static final int WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED = 0xA2;

    private MqttReasonCodes()
    {
        // Utility
    }
}
