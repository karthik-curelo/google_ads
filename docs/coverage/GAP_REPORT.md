# API Coverage - Gap Report

Generated 2026-09-07 by `scripts/coverage_scan.py`. See `PLAN.md` for method and the four platform ceilings.

| Source | Coverage | implemented | missing | partial | perm-gated | platform-limited |
|---|---|---|---|---|---|---|
| facebook_pages | 100% | 17 | 0 | 0 | 0 | 0 |
| google_ads | 73% | 30 | 11 | 0 | 0 | 0 |
| google_analytics | 58% | 61 | 41 | 3 | 0 | 0 |
| google_search_console | 100% | 20 | 0 | 0 | 0 | 0 |
| instagram_insights | 100% | 32 | 0 | 0 | 0 | 0 |
| meta_ads | 92% | 48 | 3 | 1 | 0 | 0 |

## facebook_pages

Nothing outstanding.

## google_ads

| Value | Section | Capability | Note |
|---|---|---|---|
| medium | segments | segments.ad_network_type - missing |  |
| medium | segments | segments.conversion_action_name - missing |  |
| low | metrics | metrics.video_views - missing |  |
| low | metrics | metrics.interactions - missing |  |
| low | resources | asset - missing |  |
| low | resources | asset_group - missing | Performance Max |
| low | resources | change_event - missing |  |
| low | resources | label - missing |  |
| low | resources | shopping_performance_view - missing |  |
| low | segments | segments.day_of_week - missing |  |
| low | segments | segments.click_type - missing |  |

## google_analytics

| Value | Section | Capability | Note |
|---|---|---|---|
| high | dimensions | customEvent:* - partial | discovered by get_schema but not injected into requests |
| high | metrics | customEvent:* - partial |  |
| medium | dimensions | week - missing |  |
| medium | dimensions | hour - missing |  |
| medium | dimensions | appVersion - missing |  |
| medium | dimensions | pageReferrer - missing |  |
| medium | dimensions | landingPagePlusQueryString - missing |  |
| medium | dimensions | isKeyEvent - missing |  |
| medium | dimensions | sessionSourceMedium - missing |  |
| medium | dimensions | firstUserSourceMedium - missing |  |
| medium | dimensions | firstUserGoogleAdsCampaignName - missing |  |
| medium | dimensions | audienceName - missing |  |
| medium | dimensions | transactionId - missing |  |
| medium | dimensions | customUser:* - partial |  |
| medium | metrics | sessionsPerUser - missing |  |
| medium | metrics | screenPageViewsPerUser - missing |  |
| medium | metrics | eventCountPerUser - missing |  |
| medium | metrics | eventsPerSession - missing |  |
| medium | metrics | userKeyEventRate - missing |  |
| medium | metrics | sessionKeyEventRate - missing |  |
| medium | metrics | averagePurchaseRevenue - missing |  |
| medium | metrics | averageRevenuePerUser - missing |  |
| low | dimensions | year - missing |  |
| low | dimensions | month - missing |  |
| low | dimensions | isoWeek - missing |  |
| low | dimensions | nthDay - missing |  |
| low | dimensions | dayOfWeek - missing |  |
| low | dimensions | continent - missing |  |
| low | dimensions | countryId - missing |  |
| low | dimensions | operatingSystemVersion - missing |  |
| low | dimensions | streamName - missing |  |
| low | dimensions | hostName - missing |  |
| low | dimensions | fullPageUrl - missing |  |
| low | dimensions | sessionSourcePlatform - missing |  |
| low | dimensions | sessionCampaignId - missing |  |
| low | dimensions | sessionManualAdContent - missing |  |
| low | dimensions | sessionManualTerm - missing |  |
| low | dimensions | sessionGoogleAdsKeyword - missing |  |
| low | dimensions | itemBrand - missing |  |
| low | dimensions | itemListName - missing |  |
| low | dimensions | itemPromotionName - missing |  |
| low | metrics | transactionsPerPurchaser - missing |  |
| low | metrics | cartToViewRate - missing |  |
| low | metrics | purchaserRate - missing |  |

## google_search_console

Nothing outstanding.

## instagram_insights

Nothing outstanding.

## meta_ads

| Value | Section | Capability | Note |
|---|---|---|---|
| high | action_breakdowns | action_type - partial | actions[] kept raw; not split into columns |
| low | action_breakdowns | action_destination - missing |  |
| low | action_breakdowns | action_target_id - missing |  |
| low | breakdowns | hourly_stats_aggregated_by_advertiser_time_zone - missing |  |

