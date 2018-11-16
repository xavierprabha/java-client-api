package com.marklogic.client.impl;

import com.marklogic.client.MarkLogicIOException;
import okhttp3.MediaType;
import okhttp3.ResponseBody;
import org.synchronoss.cloud.nio.multipart.BlockingIOAdapter;
import org.synchronoss.cloud.nio.multipart.MultipartContext;
import org.synchronoss.cloud.nio.multipart.util.collect.CloseableIterator;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class PartIterator implements CloseableIterator<BlockingIOAdapter.Part> {
  private CloseableIterator<BlockingIOAdapter.ParserToken> parserItr;
  private BlockingIOAdapter.Part partBuffer = null;

  PartIterator(ResponseBody responseBody) {
    this(responseBody.contentType(), responseBody);
  }
  PartIterator(MediaType contentType, ResponseBody responseBody) {
    this(BlockingIOAdapter.parse(
          responseBody.byteStream(),
          new MultipartContext(
                (contentType == null) ? null : contentType.toString(), (int) responseBody.contentLength(),null
          )
    ));
  }
  private PartIterator(CloseableIterator<BlockingIOAdapter.ParserToken> wrapped) {
    this.parserItr = wrapped;
  }

  public Stream<BlockingIOAdapter.Part> stream() {
    if (!hasNext()) {
      return Stream.empty();
    }

    return StreamSupport.stream(
          Spliterators.spliteratorUnknownSize(this, Spliterator.NONNULL | Spliterator.ORDERED),
          false
    );
  }

  BlockingIOAdapter.Part peek() {
    if (partBuffer == null) {
      partBuffer = nextImpl();
    }
    return partBuffer;
  }

  @Override
  public boolean hasNext() {
    if (partBuffer != null) return true;
    if (parserItr == null) return false;
    boolean hasNext = parserItr.hasNext();
    if (!hasNext) {
      close();
    }
    return hasNext;
  }
  @Override
  public BlockingIOAdapter.Part next() {
    if (partBuffer != null) {
      BlockingIOAdapter.Part next = partBuffer;
      partBuffer = null;
      return next;
    }
    return nextImpl();
  }
  private BlockingIOAdapter.Part nextImpl() {
    if (parserItr == null) return null;
    return (BlockingIOAdapter.Part) parserItr.next();
  }
  @Override
  public void remove() {
    if (partBuffer != null) {
      partBuffer = null;
      return;
    }
    if (parserItr == null) return;
    parserItr.remove();
  }
  @Override
  public void close(){
    if (partBuffer != null) {
      partBuffer = null;
    }
    if (parserItr == null) return;
    try {
      parserItr.close();
    } catch(Exception e) {
      throw new MarkLogicIOException(e);
    } finally {
      parserItr = null;
    }
  }
}
