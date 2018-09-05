/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.client;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;

import java.io.InputStream;

public class InputStreamHolder
{
  private final InputStream stream;
  private final long length;

  private InputStreamHolder(final InputStream stream, final long length)
  {
    this.stream = stream;
    this.length = length;
  }

  public static InputStreamHolder fromStream(final InputStream stream, long length)
  {
    return new InputStreamHolder(stream, length);
  }

  public static InputStreamHolder fromChannelBuffer(final ChannelBuffer buffer)
  {
    final int length = buffer.readableBytes();
    return new InputStreamHolder(new ChannelBufferInputStream(buffer), length);
  }

  public InputStream getStream()
  {
    return stream;
  }

  public long getLength()
  {
    return length;
  }
}
