/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.metamx.common.StringUtils;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
<<<<<<< HEAD
=======
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextAction;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.ScriptableObject;
>>>>>>> refs/remotes/druid-io/master

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.List;

public class JavaScriptAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x6;

  private final String name;
  private final List<String> fieldNames;
  private final String fnAggregate;
  private final String fnReset;
  private final String fnCombine;


  private final ScriptAggregator compiledScript;

  @JsonCreator
  public JavaScriptAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("fnAggregate") final String fnAggregate,
      @JsonProperty("fnReset") final String fnReset,
      @JsonProperty("fnCombine") final String fnCombine
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldNames, "Must have a valid, non-null fieldNames");
    Preconditions.checkNotNull(fnAggregate, "Must have a valid, non-null fnAggregate");
    Preconditions.checkNotNull(fnReset, "Must have a valid, non-null fnReset");
    Preconditions.checkNotNull(fnCombine, "Must have a valid, non-null fnCombine");

    this.name = name;
    this.fieldNames = fieldNames;

    this.fnAggregate = fnAggregate;
    this.fnReset = fnReset;
    this.fnCombine = fnCombine;

    this.compiledScript = new RhinoScriptAggregatorFactory(fnAggregate, fnReset, fnCombine).compileScript();
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnFactory)
  {
    return new JavaScriptAggregator(
        name,
        Lists.transform(
            fieldNames,
            new com.google.common.base.Function<String, ObjectColumnSelector>()
            {
              @Override
              public ObjectColumnSelector apply(@Nullable String s)
              {
                return columnFactory.makeObjectColumnSelector(s);
              }
            }
        ),
        compiledScript
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory columnSelectorFactory)
  {
    return new JavaScriptBufferAggregator(
        Lists.transform(
            fieldNames,
            new com.google.common.base.Function<String, ObjectColumnSelector>()
            {
              @Override
              public ObjectColumnSelector apply(@Nullable String s)
              {
                return columnSelectorFactory.makeObjectColumnSelector(s);
              }
            }
        ),
        compiledScript
    );
  }

  @Override
  public Comparator getComparator()
  {
    return DoubleSumAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return compiledScript.combine(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new JavaScriptAggregatorFactory(name, Lists.newArrayList(name), fnCombine, fnReset, fnCombine);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other.getClass() == this.getClass()) {
      JavaScriptAggregatorFactory castedOther = (JavaScriptAggregatorFactory) other;
      if (this.fnCombine.equals(castedOther.fnCombine) && this.fnReset.equals(castedOther.fnReset)) {
        return getCombiningFactory();
      }
    }
    throw new AggregatorFactoryNotMergeableException(this, other);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Lists.transform(
        fieldNames,
        new com.google.common.base.Function<String, AggregatorFactory>()
        {
          @Override
          public AggregatorFactory apply(String input)
          {
            return new JavaScriptAggregatorFactory(input, fieldNames, fnAggregate, fnReset, fnCombine);
          }
        }
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    // handle "NaN" / "Infinity" values serialized as strings in JSON
    if (object instanceof String) {
      return Double.parseDouble((String) object);
    }
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @JsonProperty
  public String getFnAggregate()
  {
    return fnAggregate;
  }

  @JsonProperty
  public String getFnReset()
  {
    return fnReset;
  }

  @JsonProperty
  public String getFnCombine()
  {
    return fnCombine;
  }

  @Override
  public List<String> requiredFields()
  {
    return fieldNames;
  }

  @Override
  public byte[] getCacheKey()
  {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-1");
      byte[] fieldNameBytes = StringUtils.toUtf8(Joiner.on(",").join(fieldNames));
      byte[] sha1 = md.digest(StringUtils.toUtf8(fnAggregate + fnReset + fnCombine));

      return ByteBuffer.allocate(1 + fieldNameBytes.length + sha1.length)
                       .put(CACHE_TYPE_ID)
                       .put(fieldNameBytes)
                       .put(sha1)
                       .array();
    }
    catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Unable to get SHA1 digest instance", e);
    }
  }

  @Override
  public String getTypeName()
  {
    return "float";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Doubles.BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return compiledScript.reset();
  }

  @Override
  public String toString()
  {
    return "JavaScriptAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldNames=" + fieldNames +
           ", fnAggregate='" + fnAggregate + '\'' +
           ", fnReset='" + fnReset + '\'' +
           ", fnCombine='" + fnCombine + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    JavaScriptAggregatorFactory that = (JavaScriptAggregatorFactory) o;

    if (compiledScript != null ? !compiledScript.equals(that.compiledScript) : that.compiledScript != null)
      return false;
    if (fieldNames != null ? !fieldNames.equals(that.fieldNames) : that.fieldNames != null) return false;
    if (fnAggregate != null ? !fnAggregate.equals(that.fnAggregate) : that.fnAggregate != null) return false;
    if (fnCombine != null ? !fnCombine.equals(that.fnCombine) : that.fnCombine != null) return false;
    if (fnReset != null ? !fnReset.equals(that.fnReset) : that.fnReset != null) return false;
    if (name != null ? !name.equals(that.name) : that.name != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fieldNames != null ? fieldNames.hashCode() : 0);
    result = 31 * result + (fnAggregate != null ? fnAggregate.hashCode() : 0);
    result = 31 * result + (fnReset != null ? fnReset.hashCode() : 0);
    result = 31 * result + (fnCombine != null ? fnCombine.hashCode() : 0);
    result = 31 * result + (compiledScript != null ? compiledScript.hashCode() : 0);
    return result;
  }
}
