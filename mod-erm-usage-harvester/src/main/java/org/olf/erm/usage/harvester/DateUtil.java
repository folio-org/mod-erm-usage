package org.olf.erm.usage.harvester;

import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import com.google.common.base.Strings;

public class DateUtil {

  public static List<YearMonth> getYearMonths(String startStr, String endStr) {
    Objects.requireNonNull(Strings.emptyToNull(startStr));

    YearMonth max = YearMonth.now().minusMonths(1); // FIXME: parameterize
    YearMonth start = YearMonth.parse(startStr);
    YearMonth end = (Objects.isNull(Strings.emptyToNull(endStr))) ? max : YearMonth.parse(endStr);

    if (end.isAfter(max)) {
      end = max;
    }

    if (start.isAfter(max)) {
      return Collections.emptyList();
    }

    List<YearMonth> resultList = new ArrayList<>();
    YearMonth temp = YearMonth.from(start);
    while (temp.isBefore(end) || temp.equals(end)) {
      resultList.add(YearMonth.from(temp));
      temp = temp.plusMonths(1);
    }

    return resultList;
  }

  private DateUtil() {}
}
