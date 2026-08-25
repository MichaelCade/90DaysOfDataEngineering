{#
  pct(numerator, denominator, precision=1)

  A percentage with a built-in divide-by-zero guard. Returns
      CASE WHEN <denominator> > 0 THEN round(100 * <numerator> / <denominator>, <precision>) END
  so an empty denominator yields NULL instead of an error. Uses 100e0 (a *double* literal) on
  purpose — a bare 100.0 is DECIMAL in Trino and makes the result a Decimal (Day 40 gotcha).

  Args are SQL expressions passed as strings, e.g. {{ pct('hundreds', 'fifties + hundreds') }}.
#}
{% macro pct(numerator, denominator, precision=1) -%}
case when ({{ denominator }}) > 0
     then round(100e0 * ({{ numerator }}) / ({{ denominator }}), {{ precision }})
end
{%- endmacro %}
