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
package com.marklogic.client.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.marklogic.client.Page;
import com.marklogic.client.impl.BasicPage;

/** Implements com.marklogic.client.Page to test the default methods.
 **/
public class PageTest {
  class TestPage extends BasicPage<Integer> {
    TestPage(Iterator<Integer> iterator, long start, long pageSize, long totalSize) {
      super(iterator, start, pageSize, totalSize);
    }
  }
  @Test
  public void testTestPage() {
    List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    Page<Integer> page = new TestPage(list.iterator(), 1, 10, 100);
    assertEquals("Unexpected size", 10, page.size());
    assertEquals("Unexpected totalPages", 10, page.getTotalPages());
    assertEquals("Unexpected hasContent", true, page.hasContent());
    assertEquals("Unexpected hasNextPage", true, page.hasNextPage());
    assertEquals("Unexpected pageNumber", 1, page.getPageNumber());
    assertEquals("Unexpected isFirstPage", true, page.isFirstPage());
    assertEquals("Unexpected isLastPage", false, page.isLastPage());

    page = new TestPage(list.iterator(), 1, 1, 100);
    assertEquals("Unexpected size", 1, page.size());
    assertEquals("Unexpected totalPages", 100, page.getTotalPages());
    assertEquals("Unexpected hasContent", true, page.hasContent());
    assertEquals("Unexpected hasNextPage", true, page.hasNextPage());
    assertEquals("Unexpected pageNumber", 1, page.getPageNumber());
    assertEquals("Unexpected isFirstPage", true, page.isFirstPage());
    assertEquals("Unexpected isLastPage", false, page.isLastPage());

    page = new TestPage(list.iterator(), 2, 10, 100);
    assertEquals("Unexpected size", 10, page.size());
    assertEquals("Unexpected totalPages", 10, page.getTotalPages());
    assertEquals("Unexpected hasContent", true, page.hasContent());
    assertEquals("Unexpected hasNextPage", true, page.hasNextPage());
    assertEquals("Unexpected pageNumber", 1, page.getPageNumber());
    assertEquals("Unexpected isFirstPage", true, page.isFirstPage());
    assertEquals("Unexpected isLastPage", false, page.isLastPage());

    page = new TestPage(list.iterator(), 10, 10, 100);
    assertEquals("Unexpected size", 10, page.size());
    assertEquals("Unexpected totalPages", 10, page.getTotalPages());
    assertEquals("Unexpected hasContent", true, page.hasContent());
    assertEquals("Unexpected hasNextPage", true, page.hasNextPage());
    assertEquals("Unexpected pageNumber", 1, page.getPageNumber());
    assertEquals("Unexpected isFirstPage", true, page.isFirstPage());
    assertEquals("Unexpected isLastPage", false, page.isLastPage());

    page = new TestPage(list.iterator(), 12, 10, 100);
    assertEquals("Unexpected size", 10, page.size());
    assertEquals("Unexpected totalPages", 10, page.getTotalPages());
    assertEquals("Unexpected hasContent", true, page.hasContent());
    assertEquals("Unexpected hasNextPage", true, page.hasNextPage());
    assertEquals("Unexpected pageNumber", 2, page.getPageNumber());
    assertEquals("Unexpected isFirstPage", false, page.isFirstPage());
    assertEquals("Unexpected isLastPage", false, page.isLastPage());

    page = new TestPage(list.iterator(), 20, 20, 100);
    assertEquals("Unexpected size", 20, page.size());
    assertEquals("Unexpected totalPages", 5, page.getTotalPages());
    assertEquals("Unexpected hasContent", true, page.hasContent());
    assertEquals("Unexpected hasNextPage", true, page.hasNextPage());
    assertEquals("Unexpected pageNumber", 1, page.getPageNumber());
    assertEquals("Unexpected isFirstPage", true, page.isFirstPage());
    assertEquals("Unexpected isLastPage", false, page.isLastPage());

    page = new TestPage(list.iterator(), 22, 20, 100);
    assertEquals("Unexpected size", 20, page.size());
    assertEquals("Unexpected totalPages", 5, page.getTotalPages());
    assertEquals("Unexpected hasContent", true, page.hasContent());
    assertEquals("Unexpected hasNextPage", true, page.hasNextPage());
    assertEquals("Unexpected pageNumber", 2, page.getPageNumber());
    assertEquals("Unexpected isFirstPage", false, page.isFirstPage());
    assertEquals("Unexpected isLastPage", false, page.isLastPage());

    page = new TestPage(list.iterator(), 18, 20, 20);
    assertEquals("Unexpected size", 20, page.size());
    assertEquals("Unexpected totalPages", 1, page.getTotalPages());
    assertEquals("Unexpected hasContent", true, page.hasContent());
    assertEquals("Unexpected hasNextPage", false, page.hasNextPage());
    assertEquals("Unexpected pageNumber", 1, page.getPageNumber());
    assertEquals("Unexpected isFirstPage", true, page.isFirstPage());
    assertEquals("Unexpected isLastPage", true, page.isLastPage());

    page = new TestPage(list.iterator(), 905, 100, 990);
    assertEquals("Unexpected size", 90, page.size());
    assertEquals("Unexpected totalPages", 10, page.getTotalPages());
    assertEquals("Unexpected hasContent", true, page.hasContent());
    assertEquals("Unexpected hasNextPage", false, page.hasNextPage());
    assertEquals("Unexpected pageNumber", 10, page.getPageNumber());
    assertEquals("Unexpected isFirstPage", false, page.isFirstPage());
    assertEquals("Unexpected isLastPage", true, page.isLastPage());
  }
}
