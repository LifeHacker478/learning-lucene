package learning.lucene;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;

import java.nio.file.Path;
import java.nio.file.Paths;

public class SnapshotPolicyExample {
  public static void main(String[] args) throws Exception {
    Path indexPath = Paths.get("snapshot-index");

    try (Directory dir = FSDirectory.open(indexPath);
        StandardAnalyzer analyzer = new StandardAnalyzer()) {

      // 1️⃣ Base policy: keep only last commit
      IndexDeletionPolicy basePolicy = new KeepOnlyLastCommitDeletionPolicy();

      // 2️⃣ Wrap it with SnapshotDeletionPolicy
      SnapshotDeletionPolicy snapshotPolicy = new SnapshotDeletionPolicy(basePolicy);

      IndexWriterConfig config = new IndexWriterConfig(analyzer);
      config.setIndexDeletionPolicy(snapshotPolicy);

      try (IndexWriter writer = new IndexWriter(dir, config)) {
        // 3️⃣ Add a sample document
        Document doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit(); // write commit #1

        // 4️⃣ Take a snapshot of the current commit
        IndexCommit snapshot = snapshotPolicy.snapshot();
        System.out.println("Snapshot taken: " + snapshot.getGeneration());

        // 5️⃣ Add more documents and commit again
        Document doc2 = new Document();
        doc2.add(new StringField("id", "2", Field.Store.YES));
        writer.addDocument(doc2);
        writer.commit(); // write commit #2

        // At this point:
        // - KeepOnlyLastCommit would normally delete commit #1
        // - But SnapshotDeletionPolicy protects it (we snapped it)

        // 6️⃣ Release the snapshot when done (allow deletion)
        snapshotPolicy.release(snapshot);
        System.out.println("Snapshot released.");
      }

      // 7️⃣ After writer close → old commits not referenced by snapshot are deleted
    }

    System.out.println("Done. Index at: " + indexPath.toAbsolutePath());
  }
}

