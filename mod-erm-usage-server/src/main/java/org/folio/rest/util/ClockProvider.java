package org.folio.rest.util;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class ClockProvider {

  public static final String FIXED_CLOCK_STRING = "2022-03-31T10:20:30Z";
  public static final String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX";
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern(DATE_TIME_FORMAT);
  private static Clock clock = Clock.systemUTC();

  private ClockProvider() {}

  public static Instant now() {
    return Instant.now(clock);
  }

  public static String nowFormatted() {
    return now().atZone(ZoneOffset.UTC).format(DATE_TIME_FORMATTER);
  }

  public static void setClock(Clock newClock) {
    clock = newClock;
  }

  public static void setFixedClock() {
    setClock(Clock.fixed(Instant.parse(FIXED_CLOCK_STRING), ZoneOffset.UTC));
  }

  public static void resetClock() {
    clock = Clock.systemUTC();
  }
}
