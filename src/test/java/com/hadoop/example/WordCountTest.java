package com.hadoop.example;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class WordCountTest {
  @Test
  public void cleanEmbeddedSingleQuoteKeepsQuote() throws Exception {
    String clean = WordCount.clean("o'clock");
    assertEquals("o'clock", clean);
  }

  @Test
  public void cleanSingleQuotedWordRemovesBothQuotes() throws Exception {
    String clean = WordCount.clean("'boy'");
    assertEquals("boy", clean);
  }

  @Test
  public void cleanParentheticalWordRemovesBothParentheses() throws Exception {
    String clean = WordCount.clean("(what)");
    assertEquals("what", clean);
  }

  @Test
  public void cleanOpenParenthesisWordRemovesParenthesis() throws Exception {
    String clean = WordCount.clean("(what");
    assertEquals("what", clean);
  }

  @Test
  public void cleanCloseParenthesisWordRemovesParenthesis() throws Exception {
    String clean = WordCount.clean("what)");
    assertEquals("what", clean);
  }

  @Test
  public void allWordsWithInitialCapsBecomeLowerCase() throws Exception {
    String clean = WordCount.clean("Upper");
    assertEquals("upper", clean);
  }

  @Test
  public void cleanSingleQuotedWordWithEmbeddedSingleQuoteRemovesOnlyOutsideQuotes() throws Exception {
    String clean = WordCount.clean("'o'clock'");
    assertEquals("o'clock", clean);
  }
}
