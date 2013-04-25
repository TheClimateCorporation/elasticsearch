package org.elasticsearch.common.lucene.spatial.prefix;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.lucene.spatial.prefix.tree.Node;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Custom {@link TokenStream} used to convert a list of {@link Node} representing
 * a Shape, into indexable terms.
 */
public final class NodeTokenStream extends TokenStream {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    private List<Node> nodes;
    private Iterator<Node> iterator;

    @Override
    public final boolean incrementToken() throws IOException {
      clearAttributes();
      if (iterator.hasNext()) {
        Node cell = iterator.next();
        CharSequence token = cell.getTokenString();
        termAtt.append(token);
        return true;
      }
      return false;
    }

    @Override
    public void reset() throws IOException {
        iterator = nodes.iterator();
    }

    /**
     * Sets the Nodes that will be converted into their indexable form
     * 
     * @param nodes Nodes to be converted
     */
    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }
}
