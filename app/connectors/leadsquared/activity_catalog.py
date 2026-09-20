"""LeadSquared activity-type catalog — every type the account exposes.

Snapshot of `ProspectActivity.svc/ActivityTypes.Get` taken 2026-09-19 (84 types).
The API requires an explicit `ActivityEvent` on every retrieval call — there is no
"all types" query (live-verified: omitting it is an MXInvalidInputException) — so
complete extraction means one stream per type. The catalog is data, not code paths:
`connector.check_connection()` compares it with the live list on every run and
reports any type that appeared since, so a new type is surfaced rather than lost.

Stream naming: the four types that predate this catalog keep their original stream
names (record_key hashes the stream name, so renaming would duplicate every row);
all others are `activity_<code>` — the code is the stable identity, the display
name is not.
"""

from __future__ import annotations

# code -> display name (as returned by ActivityTypes.Get)
ACTIVITY_TYPES: dict[int, str] = {
    0: "Email - Opened",
    1: "Email - Link Clicked",
    2: "Website - Page Visited",
    3: "Website - Form Submitted",
    4: "Website - Tracking URL Clicked",
    5: "Email - Unsubscribed",
    6: "Email - Unsubscribe Link Clicked",
    8: "Email - Mailing Preference Link Clicked",
    9: "Email - Marked Spam",
    10: "Email - Bounced",
    11: "Email - View In Browser Link Clicked",
    12: "Email - Positive Response",
    13: "Email - Negative Response",
    14: "Email - Neutral Response",
    15: "Email - Resubscribed",
    18: "Email - Positive Inbound Email",
    19: "Website - Converted to Lead",
    20: "Website - Conversion Button Clicked",
    21: "Phone Call - Inbound",
    22: "Phone Call - Outbound",
    23: "Lead Capture",
    24: "Privacy - Cookie Consent CTA Clicked",
    25: "Privacy - Opted-in for Email",
    26: "Privacy - Opted-out from Email",
    27: "Privacy - Data Protection Request",
    28: "Privacy - Do Not Track Request",
    32: "Duplicate Opportunity Detected",
    33: "Opportunity Captured",
    34: "Opportunity Shared",
    35: "Opportunity Share Revoked",
    41: "Email - Subscribed To Newsletter",
    42: "Email - Subscribed To Promotional Emails",
    61: "Email - Unsubscribed From Newsletter",
    62: "Email - Unsubscribed From Promotional Emails",
    97: "Dynamic Form - Submission",
    98: "Payment",
    200: "Call Conversation",
    201: "Appointment Status",
    203: "WhatsApp Message",
    204: "Facebook Lead Ads Submissions",
    205: "Converse Chat",
    206: "Booking Created",
    208: "Post Booking Order Status",
    209: "Upload Prescription",
    212: "Push Notification Clicked",
    213: "Diet Consultation Created",
    214: "Diet Consultation Rescheduled",
    215: "Diet Consultation Cancelled",
    216: "FollowUp Tests Recommended",
    217: "Key Concerns & Action Plan",
    218: "Lifestyle Management",
    219: "Clinical Summary",
    220: "Diet Consultation Completed",
    222: "Diet Consultation Feedback",
    223: "Booking Cancelled",
    224: "Partial Report Uploaded",
    225: "Payment Success",
    226: "Add to Cart",
    227: "Booking Edited",
    228: "Click on Banner",
    229: "Click on Search by Test",
    230: "Login Successful",
    231: "Package Selected",
    232: "Partner with Us",
    233: "Search Initiated",
    234: "Search Result Selected",
    235: "Smart Report CTA Click",
    236: "Smart Report Generated",
    237: "Smart Report Open",
    238: "Call Us",
    239: "Cart View",
    240: "Remove Cart",
    242: "App Launched",
    244: "App Version Changed",
    245: "Web Session Started",
    246: "Payment Pending",
    247: "Call Conversation Others",
    248: "Zipteams Notes",
    249: "Zipteams Meeting",
    250: "Click on Real Stories",
    21000: "Visit - Activity",
    21501: "Widget - Form Submission",
    21502: "Landing Pages Pro - Form Submission",
    21600: "Document Generation",
}

# The original four streams, kept under their historical names.
LEGACY_STREAM_BY_EVENT: dict[int, str] = {
    206: "booking_created",
    208: "post_booking_order_status",
    223: "booking_cancelled",
    204: "facebook_lead_ads_submissions",
}


def stream_name_for_event(code: int) -> str:
    return LEGACY_STREAM_BY_EVENT.get(code, f"activity_{code}")


def event_for_stream_name(name: str) -> int | None:
    """Inverse of `stream_name_for_event`; None if `name` is not an activity stream."""
    for code, legacy in LEGACY_STREAM_BY_EVENT.items():
        if legacy == name:
            return code
    if name.startswith("activity_"):
        try:
            return int(name.removeprefix("activity_"))
        except ValueError:
            return None
    return None
