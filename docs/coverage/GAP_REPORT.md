# API Coverage - Gap Report

Generated 2026-09-09 by `scripts/coverage_scan.py`. See `PLAN.md` for method and the four platform ceilings.

| Source | Coverage | implemented | missing | partial | perm-gated | platform-limited |
|---|---|---|---|---|---|---|
| facebook_pages | 100% | 17 | 0 | 0 | 0 | 0 |
| google_ads | 81% | 46 | 11 | 0 | 1 | 0 |
| google_analytics | 59% | 62 | 40 | 3 | 0 | 0 |
| google_search_console | 100% | 20 | 0 | 0 | 0 | 0 |
| instagram_insights | 100% | 32 | 0 | 0 | 0 | 0 |
| meta_ads | 92% | 48 | 3 | 1 | 0 | 0 |

## facebook_pages

Nothing outstanding.

## google_ads

| Value | Section | Capability | Note |
|---|---|---|---|
| high | resources | auction_insight - permission_gated | segments.auction_insight_domain + 6 metrics.auction_insight_search_* ARE in v25 schema (campaign/ad_group/keyword_view) but access-restricted: this developer token gets HTTP 403 authorizationError=METRIC_ACCESS_DENIED ('the developer doesn't have access to metrics') - verified customer 9232673741 2026-09-09. Access is granted by Google per developer-token/account and must be requested through Google. Streams auction_insight_campaign_performance / _ad_group_performance implemented with permission_optional -> 0-row success now, auto-populate once access is granted |
| medium | resources | asset - missing | stream ad_group_ad_asset_performance (via ad_group_ad_asset_view); standalone asset resource still not queried directly |
| medium | resources | ad_schedule_view - missing | no hour segment; grid built from campaign_criterion.ad_schedule x campaign_hourly_performance |
| medium | segments | segments.ad_network_type - missing |  |
| low | metrics | metrics.video_views - missing |  |
| low | metrics | metrics.interactions - missing |  |
| low | resources | label - missing |  |
| low | resources | expanded_landing_page_view - missing | same numbers as landing_page_view (implemented) but per post-expansion URL variant - unbounded on high traffic, dropped deliberately |
| low | resources | managed_placement_view - missing | thin resource (only resource_name); 0 rows on this account. group_/detail_placement_view carry the actual served-placement data |
| low | resources | topic_view - missing | Display topic targeting; not used by this account, add if a Display topic-targeted campaign appears |
| low | resources | change_event - missing | P2 - VERIFIED live (real change history on customer 9232673741) but no change-history report in the screenshot scope; ready-to-drop entity stream spec in GAP_REPORT_ADDENDUM |
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

