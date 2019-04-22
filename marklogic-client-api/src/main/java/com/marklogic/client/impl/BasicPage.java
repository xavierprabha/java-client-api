/*
 * Copyright 2012-2019 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.client.impl;

import java.util.Iterator;
import com.marklogic.client.Page;

public class BasicPage<T> implements Page<T> {
  private Iterator<T> iterator;
  private long start = 1;
  private Long size = null;
  private long pageSize = 0;
  private long totalSize = 0;
  private Boolean hasContent;

  protected BasicPage() {
  }
  public BasicPage(Iterator<T> iterator) {
    this();
    setIterator(iterator);
  }
  public BasicPage(Iterator<T> iterator, long start, long pageSize, long totalSize) {
    this(iterator);
    init(start, pageSize, totalSize);
  }
  public void init(long start, long pageSize, long totalSize) {
      setStart(start);
      setPageSize(pageSize);
      setTotalSize(totalSize);
  }

  @Override
  public Iterator<T> iterator() {
    return iterator;
  }

  void setIterator(Iterator<T> iterator) {
    this.iterator = iterator;
    if (!hasContent()) {
      this.hasContent = hasNext();
    }
  }

  @Override
  public boolean hasNext() {
    return (iterator == null) ? false : iterator.hasNext();
  }

  @Override
  public T next() {
    return (iterator == null) ? null : iterator.next();
  }

  @Override
  public long getStart() {
    if (start < 1) {
        setStart(1);
    }
    return start;
  }

  public BasicPage<T> setStart(long start) {
    this.start = start;
    return this;
  }

  @Override
  public long getPageSize() {
    if (pageSize == -1) {
      setPageSize(size());
    }
    return pageSize;
  }

  public BasicPage<T> setPageSize(long pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  @Override
  public long getTotalSize() {
    if (totalSize == -1) {
      setTotalSize(size());
    }
    return totalSize;
  }

  public BasicPage<T> setTotalSize(long totalSize) {
    this.totalSize = totalSize;
    return this;
  }

  public BasicPage<T> setSize(long size) {
    this.size = Long.valueOf(size);
    if (size > 0 && !hasContent()) {
      hasContent = true;
    }
    if (pageSize < size) {
      pageSize = size;
    }
    if (totalSize < size) {
      totalSize = size;
    }
    return this;
  }

  @Override
  public long size() {
    if ( size != null ) return size.longValue();
    if ( getPageSize() == 0 ) {
      return 0;
    } else if (hasNextPage() || (getTotalSize() % getPageSize()) == 0) {
      return getPageSize();
    } else {
      return getTotalSize() % getPageSize();
    }
  }

  @Override
  public long getTotalPages() {
    if ( getPageSize() == 0 ) return 0;
    return (long) Math.ceil((double) getTotalSize() / (double) getPageSize());
  }

  @Override
  public boolean hasContent() {
    return (hasContent != null) && hasContent;
  }

  @Override
  public boolean hasNextPage() {
    return getPageNumber() < getTotalPages();
  }

  @Override
  public boolean hasPreviousPage() {
    return getPageNumber() > 1;
  }

  @Override
  public long getPageNumber() {
    if (getPageSize() == 0) {
      return hasContent() ? 1 : 0;
    }
    double start    = getStart();
    double pageSize = getPageSize();
    return (start % pageSize == 0) ?
            Double.valueOf(start / pageSize).longValue() :
            (long) Math.floor(start / pageSize) + 1;
  }

  @Override
  public boolean isFirstPage() {
    if ( getPageSize() == 0 ) return true;
    return getPageNumber() == 1;
  }

  @Override
  public boolean isLastPage() {
    if ( getPageSize() == 0 ) return true;
    return getPageNumber() == getTotalPages();
  }
}
