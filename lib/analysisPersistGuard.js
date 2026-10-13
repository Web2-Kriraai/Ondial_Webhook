/**
 * One call_analysis doc per real call.
 *
 * - Match an existing doc by any alias (call_id / input_call_id / carrier_call_id) before
 *   inserting, so Telnyx dialer UUID vs v3 call_control_id runs land on the same doc.
 * - Re-runs never touch followUpProcessed (only $setOnInsert on first insert) and never
 *   overwrite a WhatsApp-edited analysis_data.callback_requested (source: "whatsapp_ai").
 */

const WHATSAPP_CALLBACK_SOURCE = "whatsapp_ai";

function hasWhatsappEditedCallback(doc) {
    return doc?.analysis_data?.callback_requested?.source === WHATSAPP_CALLBACK_SOURCE;
}

/** analysis_data for a re-run: fresh API result, but keep a WhatsApp-edited callback_requested. */
function mergeAnalysisDataForRerun(existingDoc, freshAnalysisData) {
    if (!hasWhatsappEditedCallback(existingDoc)) return freshAnalysisData;
    const base =
        freshAnalysisData && typeof freshAnalysisData === "object" ? freshAnalysisData : {};
    return { ...base, callback_requested: existingDoc.analysis_data.callback_requested };
}

const EXISTING_PROJECTION = {
    _id: 1,
    call_id: 1,
    followUpProcessed: 1,
    "analysis_data.callback_requested": 1,
};

async function findExistingAnalysisDoc(coll, aliases) {
    if (!aliases.length) return null;
    // call_id first (indexed); carrier / input ids only when that misses.
    return (
        (await coll.findOne({ call_id: { $in: aliases } }, { projection: EXISTING_PROJECTION })) ||
        (await coll.findOne(
            { $or: [{ carrier_call_id: { $in: aliases } }, { input_call_id: { $in: aliases } }] },
            { projection: EXISTING_PROJECTION }
        ))
    );
}

/**
 * @param {import('mongodb').Db} db
 * @param {{ persistCallId: string, aliases: string[], updateObj: object, now?: Date }} args
 * @returns {Promise<{ action: "inserted"|"updated", callId: string, preservedWhatsappCallback?: boolean }>}
 */
async function persistCallAnalysisDoc(db, { persistCallId, aliases = [], updateObj, now = new Date() }) {
    const coll = db.collection("call_analysis");
    const ids = [...new Set([persistCallId, ...aliases].filter(Boolean).map(String))];

    for (let pass = 0; pass < 2; pass++) {
        const existing = await findExistingAnalysisDoc(coll, ids);

        if (!existing) {
            await coll.updateOne(
                { call_id: persistCallId },
                {
                    $set: updateObj,
                    $setOnInsert: { created_at: now, followUpProcessed: false },
                },
                { upsert: true }
            );
            return { action: "inserted", callId: persistCallId };
        }

        // Keep the existing doc's identity; never reset followUpProcessed.
        const set = { ...updateObj };
        delete set.call_id;
        delete set.internalCallId;
        delete set.followUpProcessed;
        const preserve = hasWhatsappEditedCallback(existing);
        set.analysis_data = mergeAnalysisDataForRerun(existing, updateObj.analysis_data);

        const filter = { _id: existing._id };
        // Guard against a WhatsApp edit landing between read and write.
        if (!preserve) filter["analysis_data.callback_requested.source"] = { $ne: WHATSAPP_CALLBACK_SOURCE };

        const res = await coll.updateOne(filter, { $set: set });
        if (res.matchedCount > 0) {
            return {
                action: "updated",
                callId: existing.call_id || persistCallId,
                preservedWhatsappCallback: preserve,
            };
        }
        // WhatsApp edited it meanwhile → re-read and merge once more.
    }
    throw new Error("call_analysis persist lost a race twice");
}

module.exports = {
    WHATSAPP_CALLBACK_SOURCE,
    hasWhatsappEditedCallback,
    mergeAnalysisDataForRerun,
    findExistingAnalysisDoc,
    persistCallAnalysisDoc,
};
