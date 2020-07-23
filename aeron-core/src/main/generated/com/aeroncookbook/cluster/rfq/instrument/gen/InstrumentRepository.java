package com.aeroncookbook.cluster.rfq.instrument.gen;

import java.lang.Boolean;
import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.CRC32;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.Object2ObjectHashMap;

public final class InstrumentRepository {
  /**
   * The internal MutableDirectBuffer holding capacity instances.
   */
  private final ExpandableDirectByteBuffer internalBuffer;

  /**
   * For mapping the key to the offset.
   */
  private final Int2IntHashMap offsetByKey;

  /**
   * Keeps track of valid offsets.
   */
  private final IntHashSet validOffsets;

  /**
   * Used to compute CRC32 of the underlying buffer
   */
  private final CRC32 crc32 = new CRC32();

  /**
   * The current max offset used of the buffer.
   */
  private int maxUsedOffset = 0;

  /**
   * The current count of elements in the buffer.
   */
  private int currentCount = 0;

  /**
   * The maximum count of elements in the buffer.
   */
  private final int maxCapacity;

  /**
   * The iterator for unfiltered items.
   */
  private final UnfilteredIterator unfilteredIterator;

  /**
   * The length of the internal buffer.
   */
  private final int repositoryBufferLength;

  /**
   * The flyweight used by the repository.
   */
  private Instrument flyweight = null;

  /**
   * The flyweight used by the repository for reads during append from buffer operations.
   */
  private Instrument appendFlyweight = null;

  /**
   * Holds the index data for the securityId field.
   */
  private Object2ObjectHashMap<Integer, IntHashSet> indexDataForSecurityId = new Object2ObjectHashMap<Integer, IntHashSet>();

  /**
   * Holds the reverse index data for the securityId field.
   */
  private Int2ObjectHashMap<Integer> reverseIndexDataForSecurityId = new Int2ObjectHashMap<Integer>();

  /**
   * Holds the index data for the cusip field.
   */
  private Object2ObjectHashMap<String, IntHashSet> indexDataForCusip = new Object2ObjectHashMap<String, IntHashSet>();

  /**
   * Holds the reverse index data for the cusip field.
   */
  private Int2ObjectHashMap<String> reverseIndexDataForCusip = new Int2ObjectHashMap<String>();

  /**
   * Holds the index data for the enabled field.
   */
  private Object2ObjectHashMap<Boolean, IntHashSet> indexDataForEnabled = new Object2ObjectHashMap<Boolean, IntHashSet>();

  /**
   * Holds the reverse index data for the enabled field.
   */
  private Int2ObjectHashMap<Boolean> reverseIndexDataForEnabled = new Int2ObjectHashMap<Boolean>();

  /**
   * constructor
   * @param capacity capacity to build.
   */
  private InstrumentRepository(int capacity) {
    flyweight = new Instrument();
    appendFlyweight = new Instrument();
    maxCapacity = capacity;
    repositoryBufferLength = capacity * Instrument.BUFFER_LENGTH;
    internalBuffer = new ExpandableDirectByteBuffer(repositoryBufferLength);
    internalBuffer.setMemory(0, repositoryBufferLength, (byte)0);
    offsetByKey = new Int2IntHashMap(Integer.MIN_VALUE);
    validOffsets = new IntHashSet();
    unfilteredIterator = new UnfilteredIterator();
    flyweight.setIndexNotifierForSecurityId(this::updateIndexForSecurityId);
    flyweight.setIndexNotifierForCusip(this::updateIndexForCusip);
    flyweight.setIndexNotifierForEnabled(this::updateIndexForEnabled);
  }

  /**
   * Creates a respository holding at most capacity elements.
   */
  public static InstrumentRepository createWithCapacity(int capacity) {
    return new InstrumentRepository(capacity);
  }

  /**
   * Appends an element in the buffer with the provided key. Key cannot be changed. Returns null if new element could not be created or if the key already exists.
   */
  public Instrument appendWithKey(int id) {
    if (currentCount >= maxCapacity) {
      return null;
    }
    if (offsetByKey.containsKey(id)) {
      return null;
    }
    flyweight.setUnderlyingBuffer(internalBuffer, maxUsedOffset);
    offsetByKey.put(id, maxUsedOffset);
    validOffsets.add(maxUsedOffset);
    flyweight.writeHeader();
    flyweight.writeId(id);
    flyweight.lockKeyId();
    currentCount += 1;
    maxUsedOffset = maxUsedOffset + Instrument.BUFFER_LENGTH + 1;
    return flyweight;
  }

  /**
   * Appends an element in the buffer by copying over from source buffer. Returns null if new element could not be created or if the key already exists.
   */
  public Instrument appendByCopyFromBuffer(DirectBuffer buffer, int offset) {
    if (currentCount >= maxCapacity) {
      return null;
    }
    appendFlyweight.setUnderlyingBuffer(buffer, offset);
    if (offsetByKey.containsKey(appendFlyweight.readId())) {
      return null;
    }
    flyweight.setUnderlyingBuffer(internalBuffer, maxUsedOffset);
    offsetByKey.put(appendFlyweight.readId(), maxUsedOffset);
    validOffsets.add(maxUsedOffset);
    internalBuffer.putBytes(maxUsedOffset, buffer, offset, Instrument.BUFFER_LENGTH);
    flyweight.lockKeyId();
    currentCount += 1;
    updateIndexForSecurityId(maxUsedOffset, appendFlyweight.readSecurityId());
    updateIndexForCusip(maxUsedOffset, appendFlyweight.readCusip());
    updateIndexForEnabled(maxUsedOffset, appendFlyweight.readEnabled());
    maxUsedOffset = maxUsedOffset + Instrument.BUFFER_LENGTH + 1;
    return flyweight;
  }

  /**
   * Returns true if the given key is known; false if not.
   */
  public boolean containsKey(int id) {
    return offsetByKey.containsKey(id);
  }

  /**
   * Returns the number of elements currently in the repository.
   */
  public int getCurrentCount() {
    return currentCount;
  }

  /**
   * Returns the maximum number of elements that can be stored in the repository.
   */
  public int getCapacity() {
    return maxCapacity;
  }

  /**
   * Returns the internal buffer as a byte[]. Warning! Allocates.
   */
  private byte[] dumpBuffer() {
    byte[] tmpBuffer = new byte[repositoryBufferLength];
    internalBuffer.getBytes(0, tmpBuffer);
    return tmpBuffer;
  }

  /**
   * Moves the flyweight onto the buffer segment associated with the provided key. Returns null if not found.
   */
  public Instrument getByKey(int id) {
    if (offsetByKey.containsKey(id)) {
      int offset = offsetByKey.get(id);
      flyweight.setUnderlyingBuffer(internalBuffer, offset);
      flyweight.lockKeyId();
      return flyweight;
    }
    return null;
  }

  /**
   * Moves the flyweight onto the buffer segment for the provided 0-based buffer index. Returns null if not found.
   */
  public Instrument getByBufferIndex(int index) {
    if ((index + 1) <= currentCount) {
      int offset = index + (index * flyweight.BUFFER_LENGTH);
      flyweight.setUnderlyingBuffer(internalBuffer, offset);
      flyweight.lockKeyId();
      return flyweight;
    }
    return null;
  }

  /**
   * Returns offset of given 0-based index, or -1 if invalid.
   */
  public int getOffsetByBufferIndex(int index) {
    if ((index + 1) <= currentCount) {
      return index + (index * flyweight.BUFFER_LENGTH);
    }
    return -1;
  }

  /**
   * Moves the flyweight onto the buffer offset, but only if it is a valid offset. Returns null if the offset is invalid.
   */
  public Instrument getByBufferOffset(int offset) {
    if (validOffsets.contains(offset)) {
      flyweight.setUnderlyingBuffer(internalBuffer, offset);
      flyweight.lockKeyId();
      return flyweight;
    }
    return null;
  }

  /**
   * Returns the underlying buffer.
   */
  public DirectBuffer getUnderlyingBuffer() {
    return internalBuffer;
  }

  /**
   * Returns the CRC32 of the underlying buffer. Warning! Allocates.
   */
  public long getCrc32() {
    crc32.reset();
    crc32.update(dumpBuffer());
    return crc32.getValue();
  }

  /**
   * Returns iterator which returns all items. 
   */
  public Iterator<Instrument> allItems() {
    return unfilteredIterator;
  }

  /**
   * Accepts a notification that a flyweight's indexed field has been modified
   */
  private void updateIndexForSecurityId(int offset, Integer value) {
    if (reverseIndexDataForSecurityId.containsKey(offset)) {
      int oldValue = reverseIndexDataForSecurityId.get(offset);
      if (!reverseIndexDataForSecurityId.get(offset).equals(value)) {
        indexDataForSecurityId.get(oldValue).remove(offset);
      }
    }
    if (indexDataForSecurityId.containsKey(value)) {
      indexDataForSecurityId.get(value).add(offset);
    } else {
      final IntHashSet items = new IntHashSet();
      items.add(offset);
      indexDataForSecurityId.put(value, items);
    }
    reverseIndexDataForSecurityId.put(offset, value);
  }

  /**
   * Uses index to return list of offsets matching given value.
   */
  public List<Integer> getAllWithIndexSecurityIdValue(Integer value) {
    List<Integer> results = new ArrayList<Integer>();
    if (indexDataForSecurityId.containsKey(value)) {
      results.addAll(indexDataForSecurityId.get(value));
    }
    return results;
  }

  /**
   * Accepts a notification that a flyweight's indexed field has been modified
   */
  private void updateIndexForCusip(int offset, String value) {
    if (reverseIndexDataForCusip.containsKey(offset)) {
      String oldValue = reverseIndexDataForCusip.get(offset);
      if (!reverseIndexDataForCusip.get(offset).equalsIgnoreCase(value)) {
        indexDataForCusip.get(oldValue).remove(offset);
      }
    }
    if (indexDataForCusip.containsKey(value)) {
      indexDataForCusip.get(value).add(offset);
    } else {
      final IntHashSet items = new IntHashSet();
      items.add(offset);
      indexDataForCusip.put(value, items);
    }
    reverseIndexDataForCusip.put(offset, value);
  }

  /**
   * Uses index to return list of offsets matching given value.
   */
  public List<Integer> getAllWithIndexCusipValue(String value) {
    List<Integer> results = new ArrayList<Integer>();
    if (indexDataForCusip.containsKey(value)) {
      results.addAll(indexDataForCusip.get(value));
    }
    return results;
  }

  /**
   * Accepts a notification that a flyweight's indexed field has been modified
   */
  private void updateIndexForEnabled(int offset, Boolean value) {
    if (reverseIndexDataForEnabled.containsKey(offset)) {
      boolean oldValue = reverseIndexDataForEnabled.get(offset);
      if (!reverseIndexDataForEnabled.get(offset).booleanValue() == value) {
        indexDataForEnabled.get(oldValue).remove(offset);
      }
    }
    if (indexDataForEnabled.containsKey(value)) {
      indexDataForEnabled.get(value).add(offset);
    } else {
      final IntHashSet items = new IntHashSet();
      items.add(offset);
      indexDataForEnabled.put(value, items);
    }
    reverseIndexDataForEnabled.put(offset, value);
  }

  /**
   * Uses index to return list of offsets matching given value.
   */
  public List<Integer> getAllWithIndexEnabledValue(Boolean value) {
    List<Integer> results = new ArrayList<Integer>();
    if (indexDataForEnabled.containsKey(value)) {
      results.addAll(indexDataForEnabled.get(value));
    }
    return results;
  }

  private final class UnfilteredIterator implements Iterator<Instrument> {
    private Instrument iteratorFlyweight = new Instrument();

    private int currentOffset = 0;

    @Override
    public boolean hasNext() {
      return currentCount != 0 && (currentOffset + Instrument.BUFFER_LENGTH + 1 <=maxUsedOffset);
    }

    @Override
    public Instrument next() {
      if (hasNext()) {
        if (currentOffset > maxUsedOffset) {
          throw new java.util.NoSuchElementException();
        }
        iteratorFlyweight.setUnderlyingBuffer(internalBuffer, currentOffset);
        currentOffset = currentOffset + Instrument.BUFFER_LENGTH + 1;
        return iteratorFlyweight;
      }
      throw new java.util.NoSuchElementException();
    }

    public UnfilteredIterator reset() {
      currentOffset = 0;
      return this;
    }
  }
}
