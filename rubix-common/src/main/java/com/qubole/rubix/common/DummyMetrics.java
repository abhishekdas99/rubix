/**
 * Copyright (c) 2018. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.qubole.rubix.common;

/**
 * Created by Abhishek on 3/12/18.
 */
public class DummyMetrics implements Metrics
{
  public void close() throws Exception
  {
    return;
  }

  public Long incrementCounter(String name)
  {
    return new Long(0);
  }

  public Long incrementCounter(String name, long increment)
  {
    return new Long(0);
  }

  public Long decrementCounter(String name)
  {
    return new Long(0);
  }

  public Long decrementCounter(String name, long decrement)
  {
    return new Long(0);
  }

  public void addGauge(String name, final MetricsVariable variable)
  {
    return;
  }
}
