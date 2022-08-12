/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.journal;

import alluxio.exception.status.UnavailableException;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Context for merging journal entries together for a wrapped journal context.
 *
 * This is used so that we can combine several journal entries into one using a merge
 * function. This helps us resolve the inconsistency between primary and standby and
 * decrease the chance for a standby to see intermediate state of a file system operation.
 */
@NotThreadSafe
public final class FileSystemMergeJournalContext implements JournalContext {
  // It will log a warning if the number of buffered journal entries exceed 100
  public static final int MAX_ENTRIES = 100;

  private static final Logger LOG = LoggerFactory.getLogger(MergeJournalContext.class);

  private final JournalContext mJournalContext;
  private final UnaryOperator<List<JournalEntry>> mMergeOperator;
  private final List<JournalEntry> mJournalEntries = new ArrayList<>();

  /**
   * Constructs a {@link FileSystemMergeJournalContext}.
   * @param journalContext the journal context to wrap
   * @param mergeOperator merging function which will merge multiple journal entries into one
   */
  public FileSystemMergeJournalContext(JournalContext journalContext,
                             UnaryOperator<List<JournalEntry>> mergeOperator) {
    Preconditions.checkNotNull(journalContext, "journalContext");
    mJournalContext = journalContext;
    mMergeOperator = mergeOperator;
  }

  /**
   * Captures all journals so that we can merge them later.
   * @param entry the {@link JournalEntry} to append to the journal
   */
  @Override
  public void append(JournalEntry entry) {
    mJournalEntries.add(entry);
  }

  @Override
  public void close() throws UnavailableException {
    try {
      mergeAndAppendJournals();
    } finally {
      mJournalContext.close();
    }
  }

  /**
   * Merges all journals and then flushes them.
   * The journal writer will commit these journals synchronously.
   */
  @Override
  public void flush() throws UnavailableException {
    // Skip flushing the journal if no journal entries to append
    if (mergeAndAppendJournals()) {
      mJournalContext.flush();
    }
  }

  private boolean mergeAndAppendJournals() {
    if (mJournalEntries.isEmpty()) {
      return false;
    }
    if (mJournalEntries.size() > MAX_ENTRIES) {
      LOG.debug("MergeJournalContext has " + mJournalEntries.size()
          + " entries, over the limit of " + MAX_ENTRIES);
    }
    List<JournalEntry> mergedEntries = mMergeOperator.apply(mJournalEntries);
    mergedEntries.forEach(mJournalContext::append);
    mJournalEntries.clear();
    return true;
  }
}
