require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const { connectDB } = require("../db");
const { resolveCampaignIdFromContact } = require("../lib/resolveCampaignId");
const { tryDeductCampaignCallCredits } = require("../lib/campaignCreditDeduction");

const CALL_ID = process.argv[2] || "53433625-9eb8-401e-96e6-e1228b3ff50f";
const CONTACT_ID = process.argv[3] || "6a169c9855ce49c2f99de43e";
const DURATION = Number(process.argv[4] || 23);

(async () => {
    await connectDB();
    const campaignId = await resolveCampaignIdFromContact(CONTACT_ID);
    console.log("campaignId:", campaignId);
    const result = await tryDeductCampaignCallCredits({
        callUniqueId: CALL_ID,
        contactId: CONTACT_ID,
        campaignId,
        durationSec: DURATION,
    });
    console.log("credit result:", result);
    process.exit(0);
})().catch((e) => {
    console.error(e);
    process.exit(1);
});
