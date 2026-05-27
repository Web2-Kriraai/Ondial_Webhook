require("dotenv").config();
const { MongoClient } = require("mongodb");

const ids = process.argv.slice(2);
const camp = process.env.CHECK_CAMPAIGN_ID || "6a16bf3323f0342e912eff06";

(async () => {
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db();

    for (const id of ids) {
        const filter = { $or: [{ call_id: id }, { lead_id: id }] };
        const tc = await db.collection("TestCall").findOne(filter);
        const cl = await db.collection("CallLogs").findOne(filter);
        console.log("---", id);
        console.log(
            "TestCall",
            tc
                ? {
                      campaign_id: tc.campaign_id,
                      contact_id: tc.contact_id,
                      status: tc.status,
                      creditsDeducted: tc.creditsDeducted,
                      to_number: tc.to_number,
                  }
                : null
        );
        console.log(
            "CallLogs",
            cl
                ? {
                      campaign_id: cl.campaign_id,
                      contact_id: cl.contact_id,
                      status: cl.status,
                      creditsDeducted: cl.creditsDeducted,
                      to_number: cl.to_number,
                  }
                : null
        );
    }

    const recent = await db
        .collection("TestCall")
        .find({ campaign_id: camp })
        .sort({ createdAt: -1 })
        .limit(5)
        .toArray();
    console.log("\nRecent TestCall for campaign", camp);
    for (const t of recent) {
        console.log({
            call_id: t.call_id,
            lead_id: t.lead_id,
            to: t.to_number,
            status: t.status,
            creditsDeducted: t.creditsDeducted,
            createdAt: t.createdAt,
        });
    }

    await client.close();
})().catch((e) => {
    console.error(e);
    process.exit(1);
});
