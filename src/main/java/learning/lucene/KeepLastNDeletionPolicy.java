package learning.lucene;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;


public class KeepLastNDeletionPolicy extends IndexDeletionPolicy {
  private final int numToKeep;
  public KeepLastNDeletionPolicy(int n) { this.numToKeep = n; }

  @Override
  public void onCommit(List<? extends IndexCommit> commits) throws IOException {
    for (int i = 0; i < commits.size() - numToKeep; i++) {
      commits.get(i).delete();
    }
  }

  @Override
  public void onInit(List<? extends IndexCommit> commits) throws IOException {
    onCommit(commits);
  }
}

