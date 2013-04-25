/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.lucene.spatial.prefix.tree;

import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Represents a grid cell. These are not necessarily threadsafe, although new Cell("") (world cell) must be.
 *
 * @lucene.experimental
 */
public abstract class Node implements Comparable<Node> {
  /*
  Holds a byte[] and/or String representation of the cell. Both are lazy constructed from the other.
   */
  private byte[] bytes;
  private int b_off;
  private int b_len;

  private String token;//this is the only part of equality

  private SpatialPrefixTree spatialPrefixTree;

  protected Node(SpatialPrefixTree spatialPrefixTree, String token) {
    this.spatialPrefixTree = spatialPrefixTree;
    this.token = token;

    if (getLevel() == 0)
      getShape();//ensure any lazy instantiation completes to make this threadsafe
  }

  protected Node(SpatialPrefixTree spatialPrefixTree, byte[] bytes, int off, int len) {
    this.spatialPrefixTree = spatialPrefixTree;
    this.bytes = bytes;
    this.b_off = off;
    this.b_len = len;
  }

  public void reset(byte[] bytes, int off, int len) {
    assert getLevel() != 0;
    token = null;
    this.bytes = bytes;
    this.b_off = off;
    this.b_len = len;
  }

  public String getTokenString() {
    if (token == null) {
      token = new String(bytes, b_off, b_len, SpatialPrefixTree.UTF8);
    }
    return token;
  }

  public byte[] getTokenBytes() {
    if (bytes != null) {
      if (b_off != 0 || b_len != bytes.length) {
        throw new IllegalStateException("Not supported if byte[] needs to be recreated.");
      }
    } else {
      bytes = token.getBytes(SpatialPrefixTree.UTF8);
      b_off = 0;
      b_len = bytes.length;
    }
    return bytes;
  }

  public int getLevel() {
    return token != null ? token.length() : b_len;
  }

  //TODO add getParent() and update some algorithms to use this?
  //public Cell getParent();

  /**
   * Like {@link #getSubCells()} but with the results filtered by a shape. If that shape is a {@link com.spatial4j.core.shape.Point} then it
   * must call {@link #getSubCell(com.spatial4j.core.shape.Point)};
   * Precondition: Never called when getLevel() == maxLevel.
   *
   * @param shapeFilter an optional filter for the returned cells.
   * @return A set of cells (no dups), sorted. Not Modifiable.
   */
  public Collection<Node> getSubCells(Shape shapeFilter) {
    //Note: Higher-performing subclasses might override to consider the shape filter to generate fewer cells.
    if (shapeFilter instanceof Point) {
      return Collections.singleton(getSubCell((Point) shapeFilter));
    }
    Collection<Node> subCells = getSubCells();

    if (shapeFilter == null) {
      return subCells;
    }
    List<Node> copy = new ArrayList<Node>(subCells.size());//copy since cells contractually isn't modifiable
    for (Node cell : subCells) {
      SpatialRelation rel = cell.getShape().relate(shapeFilter);
      if (rel != SpatialRelation.DISJOINT)
        copy.add(cell);
    }
    return copy;
  }

  /**
   * Performant implementations are expected to implement this efficiently by considering the current
   * cell's boundary.
   * Precondition: Never called when getLevel() == maxLevel.
   * Precondition: this.getShape().relate(p) != DISJOINT.
   */
  public abstract Node getSubCell(Point p);

  //TODO Cell getSubCell(byte b)

  /**
   * Gets the cells at the next grid cell level that cover this cell.
   * Precondition: Never called when getLevel() == maxLevel.
   *
   * @return A set of cells (no dups), sorted. Not Modifiable.
   */
  protected abstract Collection<Node> getSubCells();

  /**
   * {@link #getSubCells()}.size() -- usually a constant. Should be >=2
   */
  public abstract int getSubCellsSize();

  public abstract Shape getShape();

  public Point getCenter() {
    return getShape().getCenter();
  }

  @Override
  public int compareTo(Node o) {
    return getTokenString().compareTo(o.getTokenString());
  }

  @Override
  public boolean equals(Object obj) {
    return !(obj == null || !(obj instanceof Node)) && getTokenString().equals(((Node) obj).getTokenString());
  }

  @Override
  public int hashCode() {
    return getTokenString().hashCode();
  }

  @Override
  public String toString() {
    return getTokenString();
  }

}
