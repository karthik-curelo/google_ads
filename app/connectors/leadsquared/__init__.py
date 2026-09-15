"""LeadSquared CRM connector (§22, §24) — leads + booking/order/cancellation activity.

See docs/coverage/LSQ_DISCOVERY_2026-09-11.md and
docs/coverage/LSQ_VERIFICATION_2026-09-11.md for the research and live
verification this connector's design is built on, and §9 of the latter for
the business decisions (authoritative revenue field, Payment Status
semantics, repeat-customer policy) deliberately left unmodeled here.
"""
