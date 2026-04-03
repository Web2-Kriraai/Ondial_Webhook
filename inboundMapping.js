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

/**
 * Find contact for an inbound call: caller phone + campaign.
 * Returns Mongo _id as contact_id and lead_id from doc when present, else contact _id string.
 */
async function resolveInboundContact({ from_number, campaign_id }) {
    const variants = mobileVariants(from_number);
    if (!variants.length || !campaign_id) return null;

    const db = getDb();
    const cid = String(campaign_id);
    const campaignFilter = [{ campaign_id: cid }];
    if (ObjectId.isValid(cid) && cid.length === 24) {
        campaignFilter.push({ campaign_id: new ObjectId(cid) });
    }

    const contact = await db.collection("contactprocessings").findOne(
        {
            mobileNumber: { $in: variants },
            $or: campaignFilter,
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

module.exports = { resolveInboundContact, mobileVariants };
