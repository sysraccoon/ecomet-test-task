WITH
  -- toDate(now()) AS report_date,
  toDate('2025-01-01') AS report_date,
  1111111 AS report_campaign_id,

  phrases_total_views_by_hours AS (
    SELECT
      phrase,
      toHour(dt) as hour,
      max(views) as views
    FROM
      phrases_views
    WHERE
      campaign_id = report_campaign_id
      AND toDate(dt) = report_date
    GROUP BY
      phrase, hour
  ),
  phrases_diff_views_by_hours AS (
    SELECT
      phrase,
      hour,
      (
        views - (lagInFrame(views, 1, views)
        OVER (PARTITION BY phrase ORDER BY hour))
      ) as diff_views
    FROM
      phrases_total_views_by_hours
    ORDER BY
      phrase,
      hour DESC
  )
SELECT 
  phrase,
  groupArray((
      hour,
      diff_views
  )) AS views_by_hour
FROM
  phrases_diff_views_by_hours
WHERE
  diff_views > 0
GROUP BY
  phrase

