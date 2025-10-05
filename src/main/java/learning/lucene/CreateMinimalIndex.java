package learning.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Step 1: Creating a Minimal Index (but covering key indexing classes)
 * Covers:
 *  - document.*: Document, Field, TextField, StringField, LongPoint,
 *                NumericDocValuesField, BinaryDocValuesField
 *  - index.*   : IndexWriter, IndexWriterConfig, IndexOptions, IndexNotFoundException
 *  - store.*   : Directory, FSDirectory
 */
public class CreateMinimalIndex {

  public static void main(String[] args) throws Exception {
    Path indexPath = Paths.get(args.length > 0 ? args[0] : "example-index");
    Files.createDirectories(indexPath);

    try (Directory dir = FSDirectory.open(indexPath)) {

      // (Optional) Demonstrate IndexNotFoundException before any commits exist.
      try {
        try (DirectoryReader ignored = DirectoryReader.open(dir)) {
          // If this opens, an index already exists from a prior run.
        }
      } catch (IndexNotFoundException inf) {
        System.out.println("No index yet (as expected on first run): " + inf.getMessage());
      }

      try (Analyzer analyzer = new StandardAnalyzer()) {
        IndexWriterConfig config = IndexWriterConfigHelper.builder(analyzer)
            .withOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
         //   .withKeepLastNDeletionPolicy(5)  // Keep last 5 commits
          //  .withRAMBufferSizeMB(256.0)
            .build();

        try (IndexWriter writer = new IndexWriter(dir, config)) {
          writer.addDocument(doc(
              "1",
              "Lucene in Action",
              "Lucene is a full-text search library for Java.",
               10,
              1699999999000L,
              "v1"));

          writer.addDocument(doc(
              "2",
              "Introduction to Lucene",
              "Index documents with fields and analyze text.",
              20,
              1700000000000L,
              "v2"));

          writer.addDocument(doc(
              "3",
              "Practical Search",
              "Build an inverted index and query it.",
              30,
              1700000001000L,
              "v3"));

          // Add a document with a completely different schema (e.g., a product record)
          writer.addDocument(productDoc(
              "SKU-1001",
              "Ergonomic keyboard with backlight",
              79.99,
              150L,
              "peripherals"));


          writer.commit();
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        System.out.println("Indexed documents: " + reader.numDocs());
      }
      System.out.println("Index created at: " + indexPath.toAbsolutePath());
    }
  }

  /**
   * Builds a Document that exercises:
   *  - StringField (id)
   *  - TextField (title/body)
   *  - Field with custom FieldType using IndexOptions (body_positions)
   *  - NumericDocValuesField (popularity)
   *  - LongPoint (timestamp as a point/range-friendly field)
   *  - BinaryDocValuesField (blob/version bytes)
   */
  private static Document doc(String id, String title, String body, long popularity, long timestamp, String blob) {
    Document d = new Document();

    // Exact-match id; stored so we can print it later if needed
    d.add(new StringField("id", id, Field.Store.YES));

    // Analyzed, stored title
    d.add(new TextField("title", title, Field.Store.YES));

    // Analyzed body, not stored (typical)
    d.add(new TextField("body", body, Field.Store.NO));

    // Custom FieldType to explicitly exercise IndexOptions (positions enabled here)
    FieldType bodyWithPositions = new FieldType();
    bodyWithPositions.setTokenized(true);
    bodyWithPositions.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    bodyWithPositions.setStored(false);
    bodyWithPositions.freeze();
    d.add(new Field("body_positions", body, bodyWithPositions));

    // Columnar numeric for scoring/sorting/aggregations
    d.add(new NumericDocValuesField("popularity", popularity));

    // A BKD-backed point field (useful for range queries)
    d.add(new LongPoint("timestamp", timestamp));

    // Columnar binary (e.g., small version tag or compact feature)
    d.add(new BinaryDocValuesField("blob", new BytesRef(blob)));

    return d;
  }

  /**
   * Builds a product-style Document demonstrating a different schema from the main doc().
   * Fields included:
   *  - sku (StringField, stored)
   *  - description (TextField, not stored)
   *  - price (DoublePoint for range queries, DoubleDocValuesField for sorting, StoredField for retrieval)
   *  - inventory (LongPoint for range queries, NumericDocValuesField for sorting/aggregations)
   *  - category (StringField exact match, not stored)
   */
  private static Document productDoc(String sku, String description, double price, long inventory, String category) {
    Document d = new Document();
    d.add(new StringField("sku", sku, Field.Store.YES));
    d.add(new TextField("description", description, Field.Store.NO));
    d.add(new DoublePoint("price", price));
    d.add(new DoubleDocValuesField("price_dv", price));
    d.add(new StoredField("price_store", price));
    d.add(new LongPoint("inventory", inventory));
    d.add(new NumericDocValuesField("inventory_dv", inventory));
    d.add(new StringField("category", category, Field.Store.NO));
    return d;
  }
}
