/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.scalar.CombineHashFunction;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.IntFunction;

import static com.facebook.presto.sql.planner.optimizations.HashGenerationOptimizer.INITIAL_HASH_VALUE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class InterpretedHashGenerator
        implements HashGenerator
{
    private final List<Type> hashChannelTypes;
    private final int[] hashChannels;

    public InterpretedHashGenerator(List<Type> hashChannelTypes, List<Integer> hashChannels)
    {
        this(hashChannelTypes, requireNonNull(hashChannels).stream().mapToInt(i -> i).toArray());
    }

    public InterpretedHashGenerator(List<Type> hashChannelTypes, int[] hashChannels)
    {
        this.hashChannels = requireNonNull(hashChannels, "hashChannels is null");
        this.hashChannelTypes = ImmutableList.copyOf(requireNonNull(hashChannelTypes, "hashChannelTypes is null"));
        checkArgument(hashChannelTypes.size() == hashChannels.length);
    }

    @Override
    public long hashPosition(int position, Page page)
    {
        return hashPosition(position, page::getBlock);
    }

    public long hashPosition(int position, IntFunction<Block> blockProvider)
    {
        long result = INITIAL_HASH_VALUE;
        for (int i = 0; i < hashChannels.length; i++) {
            Type type = hashChannelTypes.get(i);
            result = CombineHashFunction.getHash(result, TypeUtils.hashPosition(type, blockProvider.apply(hashChannels[i]), position));
        }
        return result;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hashChannelTypes", hashChannelTypes)
                .add("hashChannels", hashChannels)
                .toString();
    }
}
