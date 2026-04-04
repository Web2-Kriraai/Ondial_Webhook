const { ObjectId } = require("mongodb");
const { getDb } = require("./db");
const { normalizePhone } = require("./callMapping");

function mobileVariants(raw) {
    const mobile = normalizePhone(raw);
    if (!mobile) return [];
    const v = [mobile];
    if (mobile.length === 10) v.push(`91${mobile}`, `+91${mobile}`);
    return v;
}

function campaignOrClause(campaign_id) {
    const cid = String(campaign_id);
    const clauses = [{ campaign_id: cid }];
    if (ObjectId.isValid(cid) && cid.length === 24) {
        clauses.push({ campaign_id: new ObjectId(cid) });
    }
    return { $or: clauses };
}

/**
 * Find contact for an inbound call: caller phone + campaign.
 * Returns Mongo _id as contact_id and lead_id from doc when present, else contact _id string.
 */
async function resolveInboundContact({ from_number, campaign_id }) {
    const variants = mobileVariants(from_number);
    if (!variants.length || !campaign_id) return null;

    const db = getDb();

    const contact = await db.collection("contactprocessings").findOne(
        {
            mobileNumber: { $in: variants },
            ...campaignOrClause(campaign_id),
        },
        { sort: { updatedAt: -1, _id: -1 }, projection: { lead_id: 1 } }
    );

    if (!contact?._id) return null;

    const contact_id = String(contact._id);
    const lead_id =
        contact.lead_id != null && String(contact.lead_id).trim() !== ""
            ? String(contact.lead_id)
            : contact_id;

    return {
        contact_id,
        lead_id,
        normalizedPhone: normalizePhone(from_number) || "",
    };
}

function isMongoObjectIdString(s) {
    return typeof s === "string" && /^[a-fA-F0-9]{24}$/.test(s);
}

/**
 * Resolve inbound mapping with minimal DB load:
 * 1) If contact_id (+ optional lead_id) provided — verify ObjectId against campaign (1 findOne) or accept direct_* / pass-through
 * 2) Else from_number + campaign_id phone lookup
 */
async function resolveInboundMapping({ lead_id, contact_id, from_number, campaign_id }) {
    if (!campaign_id) return null;

    const c = contact_id != null ? String(contact_id).trim() : "";

    if (c) {
        if (c.startsWith("direct_")) {
            const digits = c.slice("direct_".length).replace(/\D/g, "");
            const lead =
                lead_id != null && String(lead_id).trim() !== ""
                    ? String(lead_id).trim()
                    : c;
            return {
                contact_id: c,
                lead_id: lead,
                normalizedPhone: normalizePhone(from_number) || digits || "",
            };
        }

        if (isMongoObjectIdString(c)) {
            const db = getDb();
            const contact = await db.collection("contactprocessings").findOne(
                {
                    _id: new ObjectId(c),
                    ...campaignOrClause(campaign_id),
                },
                { projection: { lead_id: 1 } }
            );
            if (!contact?._id) return null;

            const contact_idStr = String(contact._id);
            const lead =
                lead_id != null && String(lead_id).trim() !== ""
                    ? String(lead_id).trim()
                    : contact.lead_id != null && String(contact.lead_id).trim() !== ""
                      ? String(contact.lead_id)
                      : contact_idStr;

            return {
                contact_id: contact_idStr,
                lead_id: lead,
                normalizedPhone: normalizePhone(from_number) || "",
            };
        }

        // Opaque external id — trust caller; still need lead_id for logs
        const lead =
            lead_id != null && String(lead_id).trim() !== "" ? String(lead_id).trim() : c;
        return {
            contact_id: c,
            lead_id: lead,
            normalizedPhone: normalizePhone(from_number) || "",
        };
    }

    // lead_id-only: single lookup (optional path for dialers that only send lead_id)
    if (lead_id != null && String(lead_id).trim() !== "" && !c) {
        const db = getDb();
        const lid = String(lead_id).trim();
        const contact = await db.collection("contactprocessings").findOne(
            {
                lead_id: lid,
                ...campaignOrClause(campaign_id),
            },
            { sort: { updatedAt: -1 }, projection: { lead_id: 1 } }
        );
        if (!contact?._id) return null;
        const contact_idStr = String(contact._id);
        const lead =
            contact.lead_id != null && String(contact.lead_id).trim() !== ""
                ? String(contact.lead_id)
                : lid;
        return {
            contact_id: contact_idStr,
            lead_id: lead,
            normalizedPhone: normalizePhone(from_number) || "",
        };
    }

    if (!from_number) return null;
    return resolveInboundContact({ from_number, campaign_id });
}

module.exports = { resolveInboundContact, resolveInboundMapping, mobileVariants };
