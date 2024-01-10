package com.google.migration.partitioning;

import com.google.migration.Helpers;
import com.google.migration.dto.PartitionRange;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.values.KV;

public class UUIDHelpers {
  public static UUID bigIntToUUID(BigInteger in) {
    long low = in.longValue();
    long high = in.shiftRight(64).longValue();

    return new UUID(high, low);
  }

  public static BigInteger uuidToBigInt(UUID in) {
    // https://stackoverflow.com/questions/55752927/how-to-convert-an-unsigned-long-to-biginteger
    final BigInteger MASK_64 =
        BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    BigInteger msb = BigInteger.valueOf(in.getMostSignificantBits()).and(MASK_64).shiftLeft(64);
    BigInteger lsb = BigInteger.valueOf(in.getLeastSignificantBits()).and(MASK_64);
    BigInteger retVal = msb.or(lsb);

    return retVal;
  }

  public static PartitionRange getRangeFromList(UUID lookup, List<PartitionRange> rangeList) {
    Comparator<PartitionRange> comparator = (o1, o2) -> {
      BigInteger lhs = UUIDHelpers.uuidToBigInt(UUID.fromString(o1.getStartRange()));
      BigInteger rhs = Helpers.uuidToBigInt(UUID.fromString(o2.getStartRange()));
      return lhs.compareTo(rhs);
    };

    List<PartitionRange> sortedCopy = new ArrayList(rangeList);
    sortedCopy.sort(comparator);

    int searchIndex =
        Collections.binarySearch(sortedCopy,
            new PartitionRange(lookup.toString(), lookup.toString()),
            comparator);

    int rangeIndex = searchIndex;
    if(searchIndex < 0) rangeIndex = -searchIndex - 2;

    return sortedCopy.get(rangeIndex);
  }
} // class UUIDHelpers