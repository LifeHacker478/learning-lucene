package learning.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;

/**
 * Helper class to demonstrate various IndexWriterConfig settings.
 *
 * IndexWriterConfig controls how Lucene indexes documents, including:
 * - Text analysis (Analyzer)
 * - Memory/segment management (RAM buffer, merge policies)
 * - Index creation mode (OpenMode)
 * - Segment merging (merge scheduler, deletion policy)
 * - Scoring algorithm (Similarity)
 * - Commit behavior (commitOnClose)
 *
 * Usage patterns:
 * 1. Simple standalone configs: Use createXXX() methods for single-concern configs
 * 2. Combined configs: Use ConfigBuilder for chaining multiple settings
 * 3. Modify existing configs: Use applyXXX() methods to add settings to existing config
 */
public class IndexWriterConfigHelper {

  // ============================================================================
  // BUILDER PATTERN - Allows combining multiple configuration options
  // ============================================================================

  /**
   * Builder for creating IndexWriterConfig with multiple combined settings.
   * Allows fluent chaining of configuration options.
   *
   * Example:
   * <pre>
   * IndexWriterConfig config = IndexWriterConfigHelper.builder(analyzer)
   *     .withOpenMode(OpenMode.CREATE_OR_APPEND)
   *     .withRAMBufferSizeMB(256.0)
   *     .withTieredMergePolicy(10 * 1024, 8, 2)
   *     .withBM25Similarity()
   *     .build();
   * </pre>
   */
  public static class ConfigBuilder {
    private final IndexWriterConfig config;

    private ConfigBuilder(Analyzer analyzer) {
      this.config = new IndexWriterConfig(analyzer);
    }

    public ConfigBuilder withOpenMode(OpenMode mode) {
      config.setOpenMode(mode);
      return this;
    }

    public ConfigBuilder withRAMBufferSizeMB(double ramBufferSizeMB) {
      config.setRAMBufferSizeMB(ramBufferSizeMB);
      return this;
    }

    public ConfigBuilder withMaxBufferedDocs(int maxBufferedDocs) {
      config.setMaxBufferedDocs(maxBufferedDocs);
      return this;
    }

    public ConfigBuilder withTieredMergePolicy(double maxMergedSegmentMB, double segmentsPerTier, double floorSegmentMB) {
      TieredMergePolicy mergePolicy = new TieredMergePolicy();
      mergePolicy.setMaxMergedSegmentMB(maxMergedSegmentMB);
      mergePolicy.setSegmentsPerTier(segmentsPerTier);
      mergePolicy.setFloorSegmentMB(floorSegmentMB);
      config.setMergePolicy(mergePolicy);
      return this;
    }

    public ConfigBuilder withTieredMergePolicy() {
      return withTieredMergePolicy(5 * 1024, 10, 2); // defaults
    }

    public ConfigBuilder withLogByteSizeMergePolicy(double minMergeMB, double maxMergeMB, int mergeFactor) {
      LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
      mergePolicy.setMinMergeMB(minMergeMB);
      mergePolicy.setMaxMergeMB(maxMergeMB);
      mergePolicy.setMergeFactor(mergeFactor);
      config.setMergePolicy(mergePolicy);
      return this;
    }

    public ConfigBuilder withLogByteSizeMergePolicy() {
      return withLogByteSizeMergePolicy(1.6, 2.5 * 1024, 10); // defaults
    }

    public ConfigBuilder withMergeScheduler(int maxMergeCount, int maxThreadCount) {
      ConcurrentMergeScheduler mergeScheduler = new ConcurrentMergeScheduler();
      mergeScheduler.setMaxMergesAndThreads(maxMergeCount, maxThreadCount);
      config.setMergeScheduler(mergeScheduler);
      return this;
    }

    public ConfigBuilder withKeepOnlyLastCommitDeletionPolicy() {
      config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
      return this;
    }

    public ConfigBuilder withKeepLastNDeletionPolicy(int n) {
      config.setIndexDeletionPolicy(new KeepLastNDeletionPolicy(n));
      return this;
    }

    public ConfigBuilder withBM25Similarity() {
      config.setSimilarity(new BM25Similarity());
      return this;
    }

    public ConfigBuilder withBM25Similarity(float k1, float b) {
      config.setSimilarity(new BM25Similarity(k1, b));
      return this;
    }

    public ConfigBuilder withClassicSimilarity() {
      config.setSimilarity(new ClassicSimilarity());
      return this;
    }

    public ConfigBuilder withCommitOnClose(boolean commitOnClose) {
      config.setCommitOnClose(commitOnClose);
      return this;
    }

    public IndexWriterConfig build() {
      return config;
    }
  }

  /**
   * Creates a new ConfigBuilder for fluent configuration.
   */
  public static ConfigBuilder builder(Analyzer analyzer) {
    return new ConfigBuilder(analyzer);
  }

  // ============================================================================
  // APPLY METHODS - Modify existing IndexWriterConfig instances
  // ============================================================================

  /**
   * Applies OpenMode to an existing config.
   */
  public static IndexWriterConfig applyOpenMode(IndexWriterConfig config, OpenMode mode) {
    config.setOpenMode(mode);
    return config;
  }

  /**
   * Applies RAM buffer size to an existing config.
   */
  public static IndexWriterConfig applyRAMBufferSizeMB(IndexWriterConfig config, double ramBufferSizeMB) {
    config.setRAMBufferSizeMB(ramBufferSizeMB);
    return config;
  }

  /**
   * Applies TieredMergePolicy to an existing config.
   */
  public static IndexWriterConfig applyTieredMergePolicy(IndexWriterConfig config, double maxMergedSegmentMB, double segmentsPerTier, double floorSegmentMB) {
    TieredMergePolicy mergePolicy = new TieredMergePolicy();
    mergePolicy.setMaxMergedSegmentMB(maxMergedSegmentMB);
    mergePolicy.setSegmentsPerTier(segmentsPerTier);
    mergePolicy.setFloorSegmentMB(floorSegmentMB);
    config.setMergePolicy(mergePolicy);
    return config;
  }

  /**
   * Applies LogByteSizeMergePolicy to an existing config.
   */
  public static IndexWriterConfig applyLogByteSizeMergePolicy(IndexWriterConfig config, double minMergeMB, double maxMergeMB, int mergeFactor) {
    LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
    mergePolicy.setMinMergeMB(minMergeMB);
    mergePolicy.setMaxMergeMB(maxMergeMB);
    mergePolicy.setMergeFactor(mergeFactor);
    config.setMergePolicy(mergePolicy);
    return config;
  }

  /**
   * Applies ConcurrentMergeScheduler to an existing config.
   */
  public static IndexWriterConfig applyMergeScheduler(IndexWriterConfig config, int maxMergeCount, int maxThreadCount) {
    ConcurrentMergeScheduler mergeScheduler = new ConcurrentMergeScheduler();
    mergeScheduler.setMaxMergesAndThreads(maxMergeCount, maxThreadCount);
    config.setMergeScheduler(mergeScheduler);
    return config;
  }

  /**
   * Applies BM25Similarity to an existing config.
   */
  public static IndexWriterConfig applyBM25Similarity(IndexWriterConfig config) {
    config.setSimilarity(new BM25Similarity());
    return config;
  }

  /**
   * Applies ClassicSimilarity to an existing config.
   */
  public static IndexWriterConfig applyClassicSimilarity(IndexWriterConfig config) {
    config.setSimilarity(new ClassicSimilarity());
    return config;
  }

  /**
   * Applies commitOnClose setting to an existing config.
   */
  public static IndexWriterConfig applyCommitOnClose(IndexWriterConfig config, boolean commitOnClose) {
    config.setCommitOnClose(commitOnClose);
    return config;
  }

  // ============================================================================
  // single-concern configs (kept for learning)
  // ============================================================================

  /**
   * Creates a basic IndexWriterConfig with default settings.
   * Uses StandardAnalyzer and default configurations.
   */
  public static IndexWriterConfig createBasicConfig(Analyzer analyzer) {
    return new IndexWriterConfig(analyzer);
  }

  /**
   * Creates a config demonstrating OpenMode options.
   *
   * OpenMode determines how the IndexWriter behaves when opening an index:
   * - CREATE: Creates a new index, overwriting any existing index
   * - APPEND: Opens existing index and appends to it
   * - CREATE_OR_APPEND: Creates new if doesn't exist, otherwise appends
   */
  public static IndexWriterConfig createWithOpenMode(Analyzer analyzer, OpenMode mode) {
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setOpenMode(mode);
    return config;
  }

  /**
   * Creates a config with custom RAM buffer settings.
   *
   * RAM Buffer controls when segments are flushed to disk:
   * - setRAMBufferSizeMB: Flushes when buffer reaches size (default: 16 MB)
   * - setMaxBufferedDocs: Alternative - flush after N documents
   */
  public static IndexWriterConfig createWithRAMBufferConfig(Analyzer analyzer, double ramBufferSizeMB) {
    IndexWriterConfig config = new IndexWriterConfig(analyzer);

    // Set RAM buffer size (in MB)
    config.setRAMBufferSizeMB(ramBufferSizeMB);

    // Alternative: flush after specific number of docs
    // config.setMaxBufferedDocs(1000);

    return config;
  }

  /**
   * Creates a config with TieredMergePolicy (default and recommended).
   *
   * TieredMergePolicy merges segments of similar size, creating tiers.
   * It's efficient for most use cases with good balance of merge cost and query performance.
   */
  public static IndexWriterConfig createWithTieredMergePolicy(Analyzer analyzer) {
    IndexWriterConfig config = new IndexWriterConfig(analyzer);

    TieredMergePolicy mergePolicy = new TieredMergePolicy();
    // Max merged segment size (default: 5GB)
    mergePolicy.setMaxMergedSegmentMB(5 * 1024);
    // Segments per tier (default: 10)
    mergePolicy.setSegmentsPerTier(10);
    // Floor segment size in MB (default: 2MB)
    mergePolicy.setFloorSegmentMB(2);

    config.setMergePolicy(mergePolicy);
    return config;
  }

  /**
   * Creates a config with LogByteSizeMergePolicy.
   *
   * LogByteSizeMergePolicy merges segments based on byte size in logarithmic fashion.
   * Older alternative to TieredMergePolicy, useful for specific use cases.
   */
  public static IndexWriterConfig createWithLogByteSizeMergePolicy(Analyzer analyzer) {
    IndexWriterConfig config = new IndexWriterConfig(analyzer);

    LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
    // Minimum segment size before merging (default: 1.6 MB)
    mergePolicy.setMinMergeMB(1.6);
    // Maximum segment size for merging (default: ~2.5 GB)
    mergePolicy.setMaxMergeMB(2.5 * 1024);
    // Merge factor - how many segments to merge at once (default: 10)
    mergePolicy.setMergeFactor(10);

    config.setMergePolicy(mergePolicy);
    return config;
  }

  /**
   * Creates a config with ConcurrentMergeScheduler (default).
   *
   * ConcurrentMergeScheduler runs merges in background threads.
   * - Controls how many merge threads can run simultaneously
   * - Balances indexing throughput vs. merge overhead
   */
  public static IndexWriterConfig createWithMergeScheduler(Analyzer analyzer, int maxThreadCount, int maxMergeCount) {
    IndexWriterConfig config = new IndexWriterConfig(analyzer);

    ConcurrentMergeScheduler mergeScheduler = new ConcurrentMergeScheduler();
    // Max concurrent merge threads
    mergeScheduler.setMaxMergesAndThreads(maxMergeCount, maxThreadCount);

    config.setMergeScheduler(mergeScheduler);
    return config;
  }

  /**
   * Creates a config with KeepOnlyLastCommitDeletionPolicy (default).
   *
   * DeletionPolicy controls which commit points to keep:
   * - KeepOnlyLastCommitDeletionPolicy: Only keeps the last commit (default)
   * - NoDeletionPolicy: Keeps all commits (useful for backups/snapshots)
   * - SnapshotDeletionPolicy: Allows taking snapshots of specific commits
   */
  public static IndexWriterConfig createWithDeletionPolicy(Analyzer analyzer) {
    IndexWriterConfig config = new IndexWriterConfig(analyzer);

    // Default: keep only the last commit
    config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());

    // Alternative: NoDeletionPolicy (keep all commits)
    // config.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);

    // Alternative: SnapshotDeletionPolicy for backup support
    // config.setIndexDeletionPolicy(new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy()));

    return config;
  }

  /**
   * Creates a config with BM25Similarity (default since Lucene 6.0).
   *
   * Similarity determines the scoring algorithm for search results:
   * - BM25Similarity: Modern probabilistic model (default, recommended)
   * - ClassicSimilarity: TF-IDF based (legacy)
   * - Custom implementations possible
   */
  public static IndexWriterConfig createWithBM25Similarity(Analyzer analyzer) {
    IndexWriterConfig config = new IndexWriterConfig(analyzer);

    // BM25 with default parameters (k1=1.2, b=0.75)
    config.setSimilarity(new BM25Similarity());

    // BM25 with custom parameters
    // config.setSimilarity(new BM25Similarity(1.5f, 0.8f));

    return config;
  }

  /**
   * Creates a config with ClassicSimilarity (TF-IDF).
   *
   * Uses traditional TF-IDF scoring. Kept for compatibility with older indexes.
   */
  public static IndexWriterConfig createWithClassicSimilarity(Analyzer analyzer) {
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setSimilarity(new ClassicSimilarity());
    return config;
  }

  /**
   * Creates a config with commitOnClose setting.
   *
   * commitOnClose controls whether IndexWriter automatically commits when closed:
   * - true (default): Commits pending changes on close()
   * - false: Discards uncommitted changes on close() - requires explicit commit()
   */
  public static IndexWriterConfig createWithCommitOnClose(Analyzer analyzer, boolean commitOnClose) {
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setCommitOnClose(commitOnClose);
    return config;
  }

  /**
   * Creates a fully customized config demonstrating multiple settings together.
   * This is a production-ready configuration example.
   */
  public static IndexWriterConfig createProductionConfig(Analyzer analyzer) {
    IndexWriterConfig config = new IndexWriterConfig(analyzer);

    // 1. Analyzer: passed as parameter (e.g., StandardAnalyzer, CustomAnalyzer)

    // 2. OpenMode: Create or append
    config.setOpenMode(OpenMode.CREATE_OR_APPEND);

    // 3. RAM Buffer: Use 256 MB before flushing
    config.setRAMBufferSizeMB(256.0);

    // 4. Merge Policy: TieredMergePolicy with custom settings
    TieredMergePolicy mergePolicy = new TieredMergePolicy();
    mergePolicy.setMaxMergedSegmentMB(10 * 1024); // 10 GB max segment
    mergePolicy.setSegmentsPerTier(8);
    config.setMergePolicy(mergePolicy);

    // 5. Merge Scheduler: Allow 2 concurrent merge threads
    ConcurrentMergeScheduler mergeScheduler = new ConcurrentMergeScheduler();
    mergeScheduler.setMaxMergesAndThreads(4, 2);
    config.setMergeScheduler(mergeScheduler);

    // 6. Deletion Policy: Keep only last commit
    config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());

    // 7. Similarity: Use BM25 for scoring
    config.setSimilarity(new BM25Similarity());

    // 8. Commit on close: Auto-commit when writer closes
    config.setCommitOnClose(true);

    return config;
  }

  /**
   * Prints all configuration settings of an IndexWriterConfig.
   * Useful for debugging and understanding current settings.
   */
  public static void printConfig(IndexWriterConfig config) {
    System.out.println("=== IndexWriterConfig Settings ===");
    System.out.println("OpenMode: " + config.getOpenMode());
    System.out.println("RAM Buffer Size (MB): " + config.getRAMBufferSizeMB());
    System.out.println("Max Buffered Docs: " + config.getMaxBufferedDocs());
    System.out.println("Merge Policy: " + config.getMergePolicy().getClass().getSimpleName());
    System.out.println("Merge Scheduler: " + config.getMergeScheduler().getClass().getSimpleName());
    System.out.println("Deletion Policy: " + config.getIndexDeletionPolicy().getClass().getSimpleName());
    System.out.println("Similarity: " + config.getSimilarity().getClass().getSimpleName());
    System.out.println("Commit On Close: " + config.getCommitOnClose());
    System.out.println("Analyzer: " + config.getAnalyzer().getClass().getSimpleName());
    System.out.println("==================================");
  }
}
